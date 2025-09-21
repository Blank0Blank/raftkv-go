package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"raftkv-go/kv"
	"raftkv-go/raft"
)

var currentLeader string // Tracks leader
var kvStore *kv.Store
var raftNode *raft.Node

type statusWriter struct {
	http.ResponseWriter
	status int
}

func newStatusWriter(w http.ResponseWriter) *statusWriter {
	return &statusWriter{ResponseWriter: w, status: http.StatusOK}
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := newStatusWriter(w)
		next.ServeHTTP(sw, r)
		d := time.Since(start)
		log.Printf("%s %s -> %d in %s", r.Method, r.URL.Path, sw.status, d)
	})
}

var peers = map[string]string{
	"8000": "http://localhost:8000",
	"8080": "http://localhost:8080",
	"8081": "http://localhost:8081",
}

func initPeers(currentPort string) {
	delete(peers, currentPort)
}

// PeerReplicator implements kv.Replicator using peers map
type PeerReplicator struct {
	peers map[string]string
}

func (p *PeerReplicator) Replicate(key, value string) {
	for _, peer := range p.peers {
		go func(peerURL string) {
			url := fmt.Sprintf("%s/store/%s?replicate=false", peerURL, key)

			req, err := http.NewRequest("PUT", url, strings.NewReader(value))
			if err != nil {
				log.Printf("Error creating request to %s: %v", peerURL, err)
				return
			}

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error replicating to %s: %v", peerURL, err)
				return
			}
			defer resp.Body.Close()

			log.Printf("Replicated %s to %s", key, peerURL)
		}(peer)
	}
}

// LeaderURL returns leader URL if known.
func LeaderURL() (string, bool) {
	// Check if this node is the leader
	if raftNode.LeaderID() != "" {
		// This node is the leader, return empty string (no redirect needed)
		return "", true
	}

	// Check if we know who the leader is
	if currentLeader == "" {
		return "", false
	}
	return peers[currentLeader], true
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	serverID := port
	initPeers(port)

	// Create and start raft node
	raftNode = raft.NewNode(serverID, peers)
	raftNode.Start()

	// Create and load kv store
	kvStore = kv.New("data_" + port + ".json")
	kvStore.Replicator = &PeerReplicator{peers: peers}
	kvStore.LeaderURL = LeaderURL
	if err := kvStore.Load(); err != nil {
		log.Printf("Error loading data: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /store/{key}", kvStore.GetHandler)
	mux.HandleFunc("PUT /store/{key}", kvStore.PutHandler)
	mux.HandleFunc("DELETE /store/{key}", kvStore.DeleteHandler)
	mux.HandleFunc("POST /vote", raftNode.HandleVoteRequest)
	// Wrap heartbeat handler to update currentLeader for redirects
	mux.HandleFunc("POST /heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb map[string]interface{}
		json.NewDecoder(r.Body).Decode(&hb)
		if id, ok := hb["leaderId"].(string); ok {
			currentLeader = id
		}
		// Reconstruct the request body with the original JSON data
		jsonData, _ := json.Marshal(hb)
		r.Body = io.NopCloser(strings.NewReader(string(jsonData)))
		raftNode.HandleHeartbeat(w, r)
	})
	handler := logging(mux)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}
	log.Printf("listening on %s", port)
	log.Fatal(server.ListenAndServe())
}
