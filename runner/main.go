package main

import (
	"flag"
	raft "hw2/src"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

func main() {
	nodeID := flag.Int("id", 0, "Node ID")
	address := flag.String("address", "localhost:5030", "Node address")
	peersStr := flag.String("peers", "localhost:5030,localhost:5031,localhost:5032", "Peers")
	flag.Parse()

	peers := strings.Split(*peersStr, ",")
	node := raft.NewNode(*nodeID, *address, peers)

	router := setupRouter(node)

	srv := &http.Server{
		Addr:    *address,
		Handler: router,
	}

	go node.Run()

	log.Printf("Node listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("Server error: %v", err)
	}
}

func setupRouter(node *raft.RaftNode) *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/raft/RequestVote", node.HandleRequestVote).Methods("POST")
	router.HandleFunc("/raft/AppendEntries", node.HandleAppendEntries).Methods("POST")

	router.HandleFunc("/GetEntries", node.HandleCreate).Methods("POST")
	router.HandleFunc("/GetEntries/{key}", node.HandlePut).Methods("PUT")
	router.HandleFunc("/GetEntries/{key}", node.HandlePatch).Methods("PATCH")
	router.HandleFunc("/GetEntries/{key}", node.HandleDelete).Methods("DELETE")
	router.HandleFunc("/GetEntries/{key}", node.HandleGet).Methods("GET")

	return router
}
