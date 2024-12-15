package raft

import (
	"encoding/json"
	"hw2/common"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

var randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))

func (n *RaftNode) redirectToLeader(w http.ResponseWriter, r *http.Request) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != common.StateLeader {
		if n.leaderID != nil {
			http.Redirect(w, r, "http://"+*n.leaderID+"/GetEntries", http.StatusFound)
		} else {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
		}
		return true
	}
	return false
}

func (n *RaftNode) ChooseRandomReplica() string {
	var replicas []string
	for _, p := range n.peers {
		if p != n.address {
			replicas = append(replicas, p)
		}
	}
	if len(replicas) == 0 {
		return n.address
	}
	return replicas[randGenerator.Intn(len(replicas))]
}

func (n *RaftNode) HandleCreate(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	var content map[string]string
	if err := json.NewDecoder(r.Body).Decode(&content); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	cmd := n.store.GetCreateCommand(content)
	res, err := n.submitLeaderCommand(cmd)
	if err != nil {
		n.redirectToLeader(w, r)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(res))
}

func (n *RaftNode) HandlePut(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	key := vars["key"]
	var content map[string]string
	if err := json.NewDecoder(r.Body).Decode(&content); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	cmd := n.store.GetPutCommand(key, content)
	res, err := n.submitLeaderCommand(cmd)
	if err != nil {
		n.redirectToLeader(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *RaftNode) HandlePatch(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPatch {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	key := vars["key"]

	var patchRequest struct {
		ExpectedContent map[string]string `json:"expected_content"`
		NewContent      map[string]string `json:"new_content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&patchRequest); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	n.casMutex.Lock()
	defer n.casMutex.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	cmd := n.store.GetPatchCommand(key, patchRequest.ExpectedContent, patchRequest.NewContent)
	if cmd == nil {
		n.redirectToLeader(w, r)
		return
	}

	res, err := n.submitLeaderCommand(cmd)
	if err != nil {
		n.redirectToLeader(w, r)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *RaftNode) HandleDelete(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	key := vars["key"]

	if n.redirectToLeader(w, r) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	cmd := n.store.GetDeleteCommand(key)
	res, err := n.submitLeaderCommand(cmd)
	if err != nil {
		n.redirectToLeader(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *RaftNode) HandleGet(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	key := vars["key"]

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == common.StateLeader {
		replica := n.ChooseRandomReplica()
		http.Redirect(w, r, "http://"+replica+"/GetEntries/"+key, http.StatusFound)
		return
	}

	res, ok := n.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (n *RaftNode) HandleRequestVote(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var voteRequest common.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&voteRequest); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	voteReply := common.RequestVoteReply{Term: n.term}
	if voteRequest.Term > n.term {
		n.convertToFollower(voteRequest.Term)
	}
	if voteRequest.Term < n.term {
		voteReply.VoteGranted = false
	} else {
		lastLogIndex := len(n.log) - 1
		var lastLogTerm int
		if lastLogIndex >= 0 {
			lastLogTerm = n.log[lastLogIndex].Term
		} else {
			lastLogTerm = -1
		}
		upToDate := (voteRequest.LastLogTerm > lastLogTerm) ||
			(voteRequest.LastLogTerm == lastLogTerm && voteRequest.LastLogIndex >= lastLogIndex)
		if (n.votedFor == nil || *n.votedFor == voteRequest.CandidateID) && upToDate {
			n.votedFor = &voteRequest.CandidateID
			voteReply.VoteGranted = true
			n.resetElectionTimer()
		}
	}
	json.NewEncoder(w).Encode(voteReply)
}

func (n *RaftNode) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var appendEntriesArgs common.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&appendEntriesArgs); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	appendEntriesReply := common.AppendEntriesReply{Term: n.term}
	if appendEntriesArgs.Term > n.term {
		n.convertToFollower(appendEntriesArgs.Term)
	}
	if appendEntriesArgs.Term < n.term {
		appendEntriesReply.Success = false
		json.NewEncoder(w).Encode(appendEntriesReply)
		return
	}

	n.resetElectionTimer()
	n.leaderID = &appendEntriesArgs.LeaderID
	n.state = common.StateFollower

	if appendEntriesArgs.PrevLogIndex >= 0 {
		if appendEntriesArgs.PrevLogIndex >= len(n.log) {
			appendEntriesReply.Success = false
			appendEntriesReply.ConflictIndex = len(n.log)
			appendEntriesReply.ConflictTerm = -1
			json.NewEncoder(w).Encode(appendEntriesReply)
			return
		}
		if n.log[appendEntriesArgs.PrevLogIndex].Term != appendEntriesArgs.PrevLogTerm {
			conflictingTerm := n.log[appendEntriesArgs.PrevLogIndex].Term
			appendEntriesReply.Success = false
			appendEntriesReply.ConflictTerm = conflictingTerm
			conflictIndex := appendEntriesArgs.PrevLogIndex
			for conflictIndex > 0 && n.log[conflictIndex-1].Term == conflictingTerm {
				conflictIndex--
			}
			appendEntriesReply.ConflictIndex = conflictIndex
			json.NewEncoder(w).Encode(appendEntriesReply)
			return
		}
	}

	// Append new entries, overwriting conflicts
	index := appendEntriesArgs.PrevLogIndex + 1
	for i := 0; i < len(appendEntriesArgs.Entries); i++ {
		if index+i < len(n.log) {
			if n.log[index+i].Term != appendEntriesArgs.Entries[i].Term {
				// Delete the existing entry and all that follow it
				log.Printf("Deleting conflicting entry at index %d", index+i)
				n.log = n.log[:index+i]
				log.Printf("Appending %d entries instead", len(appendEntriesArgs.Entries[i:]))
				n.log = append(n.log, appendEntriesArgs.Entries[i:]...)
				break
			}
			// Entries are the same, no action needed
		} else {
			// Append any new entries not in the log
			log.Printf("Appending %d new entries", len(appendEntriesArgs.Entries[i:]))
			n.log = append(n.log, appendEntriesArgs.Entries[i:]...)
			break
		}
	}

	// Update commitIndex
	if appendEntriesArgs.LeaderCommit > n.commitIndex {
		n.commitIndex = min(appendEntriesArgs.LeaderCommit, len(n.log)-1)
		n.applyToStateMachine()
	}

	appendEntriesReply.Success = true
	json.NewEncoder(w).Encode(appendEntriesReply)
}
