package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
)

type Node struct {
	mu        sync.Mutex
	id        int
	state     int
	term      int
	votedFor  int
	votes     int
	peers     []string
	leader    int
	heartbeat chan bool
}

type VoteRequest struct {
	Term        int
	CandidateID int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

func (n *Node) RequestVote(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Term: n.term}
	if req.Term > n.term {
		n.term = req.Term
		n.state = Follower
		n.votedFor = -1
	}

	if (n.votedFor == -1 || n.votedFor == req.CandidateID) && req.Term >= n.term {
		resp.VoteGranted = true
		n.votedFor = req.CandidateID
	}

	json.NewEncoder(w).Encode(resp)
}

func (n *Node) sendVoteRequest(peer string) {
	req := VoteRequest{Term: n.term, CandidateID: n.id}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		log.Println(err)
		return
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/vote", peer), "application/json", &buf)
	if err != nil {
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		log.Println(err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if voteResp.VoteGranted {
		n.votes++
		if n.votes > len(n.peers)/2 {
			n.state = Leader
			n.leader = n.id
			log.Printf("Node %d became the leader", n.id)
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.votes = 1
	n.mu.Unlock()

	for _, peer := range n.peers {
		go n.sendVoteRequest(peer)
	}
}

func randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond
}

func (n *Node) run() {

	for {
		switch n.state {
		case Follower:
			select {
			case <-n.heartbeat:
			case <-time.After(randomTimeout() * time.Millisecond):
				n.startElection()
			}
		case Candidate:
			time.Sleep(150 * time.Millisecond)
			n.startElection()
		case Leader:
			for _, peer := range n.peers {
				go func(peer string) {
					http.Get(fmt.Sprintf("http://%s/heartbeat", peer))
				}(peer)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (n *Node) AppendEntries(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Term: n.term}
	if req.Term >= n.term {
		n.term = req.Term
		n.state = Follower
		n.votedFor = -1
		resp.VoteGranted = true
		n.heartbeat <- true
	}

	json.NewEncoder(w).Encode(resp)
}

func (n *Node) sendAppendEntries() {
	for _, peer := range n.peers {
		go func(peer string) {
			req := VoteRequest{Term: n.term, CandidateID: n.id}
			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(req); err != nil {
				log.Println(err)
				return
			}
			resp, err := http.Post(fmt.Sprintf("http://%s/appendEntries", peer), "application/json", &buf)
			if err != nil {
				log.Println(err)
				return
			}
			defer resp.Body.Close()

			var voteResp VoteResponse
			if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
				log.Println(err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if voteResp.Term > n.term {
				n.term = voteResp.Term
				n.state = Follower
				n.votedFor = -1
			}
		}(peer)
	}
}

func main() {
	node := &Node{
		id:        1,
		state:     Candidate,
		term:      0,
		votedFor:  -1,
		peers:     []string{"localhost:9091", "localhost:9092"},
		heartbeat: make(chan bool),
	}

	http.HandleFunc("/vote", node.RequestVote)
	go node.run()

	log.Fatal(http.ListenAndServe(":9090", nil))
}
