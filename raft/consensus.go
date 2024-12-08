package raft

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	Follower int32 = iota
	Candidate
	Leader
)

type LogEntry struct {
	term    int32
	index   int32
	command string
	key     string
	value   string
	votes   int32
}

type RaftNode struct {
	// node
	ip            string
	state         int32
	voted         int32
	votesReceived int32
	currentTerm   int32
	peers         []string
	heartbeat     bool
	// log
	log           []LogEntry
	lastAppended  int32
	lastCommitted int32
}

func NewRaftNode(ip string) *RaftNode {
	return &RaftNode{
		ip:            ip,
		state:         Follower,
		voted:         0,
		votesReceived: 0,
		currentTerm:   0,
	}
}

func (rn *RaftNode) StartElection() {
	atomic.StoreInt32(&rn.state, Candidate)
	atomic.AddInt32(&rn.currentTerm, 1)
	rn.voted = 1
	rn.votesReceived = 1

	term := atomic.LoadInt32(&rn.currentTerm)
	for _, peer := range rn.peers {
		go rn.RequestVote(peer, term)
	}
}

type Vote struct {
	Term        int32
	VoteGranted bool
}

type Term struct {
	Term int32
}

// "/request" handle is used
func (rn *RaftNode) RequestVote(address string, currTerm int32) {
	body, err := json.Marshal(Term{Term: currTerm})
	if err != nil {
		return
	}

	resp, err := http.Post(address+"/request", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var response Vote
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return
		}

		// if node requests vote but new leader exists
		if response.Term > rn.currentTerm {
			rn.StepDown()
			return
		}
		if response.VoteGranted {
			atomic.AddInt32(&rn.votesReceived, 1)
			if rn.votesReceived > int32(len(rn.peers)/2+1) {
				rn.BecomeLeader()
				return
			}
		}
	}
}

// "/request" handle handler
func (rn *RaftNode) ReceiveRequest(w http.ResponseWriter, r *http.Request) {

	response := Vote{
		Term:        rn.currentTerm,
		VoteGranted: atomic.CompareAndSwapInt32(&rn.voted, 0, 1),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (rn *RaftNode) BecomeLeader() {
	atomic.StoreInt32(&rn.state, Leader)
}

func (rn *RaftNode) StepDown() {
	atomic.StoreInt32(&rn.state, Follower)
	rn.votesReceived = 0
	rn.voted = 0

}

func (rn *RaftNode) Start() {
	http.HandleFunc("/request", rn.ReceiveRequest)

	timeZero := time.Now()
	electionTimeout := time.Now()

	for {
		time.Sleep(50 * time.Millisecond)
		switch atomic.LoadInt32(&rn.state) {
		case Follower:

			if rn.heartbeat {
				timeZero = time.Now()
				rn.heartbeat = false
				continue
			}

			if time.Since(timeZero) > time.Duration(150+rand.Intn(151))*time.Millisecond {
				rn.StartElection()
			}

		case Candidate:
			if time.Since(electionTimeout) > time.Duration(150+rand.Intn(151))*time.Millisecond {
				rn.StartElection()
			}
		case Leader:
			// Logic to send heartbeats to other nodes
		}
	}
}

/////////////////////////////////////////////// LOG REPLICATION ///////////////////////////////////////////////

type Operation struct {
	opType string
	key    string
	value  string
}

// any node
func (rn *RaftNode) ReceiveAppendEntries(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if atomic.LoadInt32(&rn.state) != Leader {
		leaderIP := "" // You need to have a way to get the current leader's IP
		http.Error(w, "Not the leader. Leader is at: "+leaderIP, http.StatusForbidden)
		return
	}

	rn.log = append(rn.log, entry)
	rn.lastAppended = entry.index

	for _, peer := range rn.peers {
		go rn.SendAppendEntries(peer, entry)
	}

	w.WriteHeader(http.StatusOK)
}

// leader
func (rn *RaftNode) AppendLog(op Operation) {
}

// leader
func (rn *RaftNode) SendAppendEntries(address string, item LogEntry) {
}

// leader
func (rn *RaftNode) ApproveEntry(address string, item LogEntry) {
}

// follower
func (rn *RaftNode) ApproveEntryHandler(w http.ResponseWriter, r *http.Request) {
}
