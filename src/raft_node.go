package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hw2/common"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type RaftNode struct {
	mu          sync.Mutex
	peers       []string // list of peer addresses
	address     string
	state       common.ConsensusState
	term        int
	votedFor    *string
	log         []common.LogEntry
	commitIndex int
	lastApplied int
	store       *common.Storage
	nextIndex   []int
	matchIndex  []int
	leaderID    *string

	electionTimer *time.Timer

	client   *http.Client
	casMutex sync.Mutex
}

func NewNode(id int, address string, peers []string) *RaftNode {
	n := &RaftNode{
		address:     address,
		peers:       peers,
		state:       common.StateFollower, // Candidate here?
		store:       common.NewStorage(),
		client:      &http.Client{Timeout: 500 * time.Millisecond},
		commitIndex: -1,
		lastApplied: -1,
	}
	n.resetElectionTimer()
	return n
}

// ////////////////////////////////////// LEADER ELECTION ////////////////////////////////////////

func (n *RaftNode) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := 150 + randGenerator.Intn(150)
	n.electionTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
}

func (n *RaftNode) Run() {
	randGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-n.electionTimer.C:
			n.startElection()
		case <-time.After(50 * time.Millisecond):
			n.mu.Lock()
			if n.state == common.StateLeader {
				n.resetElectionTimer()
				n.sendHeartbeats()
			}
			n.mu.Unlock()
		}

		// just for demo
		if randGenerator.Intn(100) == 0 {
			n.startElection()
		}
	}
}

func (n *RaftNode) startElection() {
	n.mu.Lock()
	n.state = common.StateCandidate
	n.term++
	n.votedFor = &n.address
	termAtStart := n.term
	votes := 1
	n.resetElectionTimer()
	n.mu.Unlock()

	log.Printf("Starting election, term: %d", termAtStart)

	done := make(chan struct{})
	timeout := time.After(2 * time.Second)

	for _, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go func(peer string) {
			lastLogIndex := len(n.log) - 1
			var lastLogTerm int
			if lastLogIndex >= 0 {
				lastLogTerm = n.log[lastLogIndex].Term
			} else {
				lastLogTerm = -1
			}
			args := common.RequestVoteArgs{
				Term:         termAtStart,
				CandidateID:  n.address,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := n.sendRequestVote(peer, args)
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.term {
				n.convertToFollower(reply.Term)
				close(done)
				return
			}
			if n.term == termAtStart && n.state == common.StateCandidate {
				if reply.VoteGranted {
					votes++
					if votes > len(n.peers)/2 {
						n.convertToLeader()
						close(done)
					}
				}
			}
		}(peer)
	}

	select {
	case <-done:
		// Election completed
	case <-timeout:
		log.Printf("Election timed out")
	}
}

func (n *RaftNode) convertToFollower(newTerm int) {
	log.Printf("Converting to follower, term: %d", newTerm)

	n.state = common.StateFollower
	n.term = newTerm
	n.votedFor = nil
	n.leaderID = nil
	n.resetElectionTimer()
}

func (n *RaftNode) convertToLeader() {
	log.Printf("Converting to leader, term: %d", n.term)

	n.state = common.StateLeader
	n.leaderID = &n.address
	n.nextIndex = make([]int, len(n.peers))
	n.matchIndex = make([]int, len(n.peers))
	for i := range n.peers {
		n.nextIndex[i] = len(n.log)
		n.matchIndex[i] = -1
	}
}

func (n *RaftNode) sendHeartbeats() {
	if n.state != common.StateLeader {
		return
	}

	log.Println("Sending heartbeats")
	term := n.term

	for i, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go n.sendAppendEntries(i, peer, term)
	}
}

func (n *RaftNode) sendRequestVote(peer string, args common.RequestVoteArgs) common.RequestVoteReply {
	var reply common.RequestVoteReply
	data, _ := json.Marshal(args)
	req, _ := http.NewRequest("POST", "http://"+peer+"/raft/RequestVote", bytes.NewReader(data))
	resp, err := n.client.Do(req)
	if err != nil {
		return reply
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply
}

////////////////////////////////////// LOG REPLICATION ////////////////////////////////////////

func (n *RaftNode) submitLeaderCommand(cmd *common.LogCommand) (string, error) {
	if n.state != common.StateLeader {
		n.mu.Unlock()
		return "", fmt.Errorf("not leader")
	}
	entry := common.LogEntry{Term: n.term, Command: cmd}
	log.Printf("Leader appending entry: %v", entry)
	n.log = append(n.log, entry)
	index := len(n.log) - 1
	term := n.term
	n.mu.Unlock()

	for i, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go n.sendAppendEntries(i, peer, term)
	}

	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("commit timeout")
		case <-ticker.C:
			n.mu.Lock()
			if n.commitIndex >= index {
				n.mu.Unlock()
				return cmd.ID, nil
			}
			n.mu.Unlock()
		}
	}
}

func (n *RaftNode) applyToStateMachine() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		_ = n.store.Apply(entry.Command)
	}
}

func (n *RaftNode) updateCommitIndex() {
	for i := len(n.log) - 1; i > n.commitIndex; i-- {
		matchCount := 1 // Count self
		for j := range n.peers {
			if n.peers[j] == n.address {
				continue
			}
			if n.matchIndex[j] >= i {
				matchCount++
			}
		}
		if matchCount > len(n.peers)/2 && n.log[i].Term == n.term {
			n.commitIndex = i
			n.applyToStateMachine()
			break
		}
	}
}

func (n *RaftNode) firstTermIndex(term int) int {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			for j := i; j >= 0; j-- {
				if n.log[j].Term != term {
					return j + 1
				}
			}
			return 0
		}
	}
	return -1
}

func (n *RaftNode) sendAppendEntries(peerIndex int, peer string, term int) {
	n.mu.Lock()
	if n.state != common.StateLeader || n.term != term {
		n.mu.Unlock()
		return
	}

	prevLogIndex := n.nextIndex[peerIndex] - 1
	var prevLogTerm int
	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	} else {
		prevLogTerm = -1
	}

	toSend := []common.LogEntry{}
	if n.nextIndex[peerIndex] < len(n.log) {
		toSend = n.log[n.nextIndex[peerIndex]:]
	}

	args := common.AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.address,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      toSend,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	log.Printf("Sending %d entries to %s", len(toSend), peer)

	data, _ := json.Marshal(args)
	req, _ := http.NewRequest("POST", "http://"+peer+"/raft/AppendEntries", bytes.NewReader(data))
	resp, err := n.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var reply common.AppendEntriesReply
	json.NewDecoder(resp.Body).Decode(&reply)

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.term {
		n.convertToFollower(reply.Term)
		return
	}
	if n.state != common.StateLeader || n.term != term {
		return
	}
	if reply.Success {
		n.nextIndex[peerIndex] = args.PrevLogIndex + len(toSend) + 1
		n.matchIndex[peerIndex] = n.nextIndex[peerIndex] - 1

		if len(toSend) > 0 {
			log.Printf("Updating commitIndex, peer: %s, matchIndex: %d", peer, n.matchIndex[peerIndex])
		}
		n.updateCommitIndex()
	} else {
		log.Printf("AppendEntries to %s failed, trying to reconcile, reply: %v", peer, reply)
		if reply.ConflictTerm != -1 {
			index := n.firstTermIndex(reply.ConflictTerm)
			if index >= 0 {
				n.nextIndex[peerIndex] = index
			} else {
				n.nextIndex[peerIndex] = reply.ConflictIndex
			}
		} else {
			n.nextIndex[peerIndex] = reply.ConflictIndex
		}
		go n.sendAppendEntries(peerIndex, peer, term)
	}
}
