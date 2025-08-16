package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	store = map[string]string{ //Example data
		"user1": "a",
		"user2": "b",
		"user3": "c",
	}
	storeMutex sync.RWMutex //Read write lock
)

func get(w http.ResponseWriter, r *http.Request) { //For retrieving value from key
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")

	storeMutex.RLock()
	value, exists := store[key]
	storeMutex.RUnlock()

	if !exists { //If key not found
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "key not found"})
		return
	}
	json.NewEncoder(w).Encode(value)
}

var currentLeader string //Tracks leader

func put(w http.ResponseWriter, r *http.Request) { //For writing key value to store
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")

	isReplication := r.URL.Query().Get("replicate") == "false"

	if !isReplication && state != Leader {
		if currentLeader != "" {
			leaderURL := peers[currentLeader]
			w.Header().Set("Location", leaderURL+r.URL.Path)
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{"error": "redirect to leader"})
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "no leader available"})
		return
	}

	value, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	storeMutex.Lock()
	store[key] = string(value)
	storeMutex.Unlock()

	if err := saveToFile(); err != nil {
		log.Printf("Error saving: %v", err)
	}

	if !isReplication {
		replicateToPeers(key, string(value))
	}

	json.NewEncoder(w).Encode(map[string]string{"info": "key value saved"})
}

func del(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")
	delete(store, key)
	if err := saveToFile(); err != nil {
		log.Printf("error saving: %v", err)
	}
	json.NewEncoder(w).Encode("deleted")
}

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

var fileName string

func saveToFile() error {
	storeMutex.RLock()
	data, err := json.Marshal(store)
	storeMutex.RUnlock()

	if err != nil {
		return err
	}
	return os.WriteFile(fileName, data, 0644)
}

func loadFromFile() error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &store)
}

var peers = map[string]string{
	"8000": "http://localhost:8000",
	"8080": "http://localhost:8080",
	"8081": "http://localhost:8081",
}

func initPeers(currentPort string) {
	delete(peers, currentPort)
}

func replicateToPeers(key, value string) {
	for _, peer := range peers {
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

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

var (
	currentTerm int
	state       ServerState = Follower
	votedFor    string
	serverID    string

	electionTimeout time.Duration
	lastHeartbeat   time.Time
)

func startElection() {
	currentTerm++
	state = Candidate
	votedFor = serverID
	votes := 1

	log.Printf("Server %s starting election for term %d", serverID, currentTerm)

	for _, peer := range peers {
		go requestVote(peer, &votes)
	}
}

var electionMutex sync.Mutex

func requestVote(peerURL string, votes *int) {
	url := fmt.Sprintf("%s/vote", peerURL)

	voteReq := map[string]interface{}{
		"term":        currentTerm,
		"candidateId": serverID,
	}
	reqBody, _ := json.Marshal(voteReq)

	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("Failed to request vote from %s: %v", peerURL, err)
		return
	}
	defer resp.Body.Close()

	var voteResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&voteResp)

	if voteResp["voteGranted"] == true {
		electionMutex.Lock()
		*votes++
		log.Printf("Got vote from %s, total votes: %d", peerURL, *votes)
		if *votes > len(peers)/2 {
			becomeLeader()
		}
		electionMutex.Unlock()
	}
}

func handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var voteReq map[string]interface{}
	json.NewDecoder(r.Body).Decode(&voteReq)

	candidateTerm := int(voteReq["term"].(float64))
	candidateID := voteReq["candidateId"].(string)

	voteGranted := false

	if votedFor == "" || candidateTerm > currentTerm {
		votedFor = candidateID
		currentTerm = candidateTerm
		voteGranted = true
		log.Printf("Granted vote to %s for term %d", candidateID, candidateTerm)
	}
	response := map[string]interface{}{
		"voteGranted": voteGranted,
		"term":        currentTerm,
	}
	json.NewEncoder(w).Encode(response)
}

func becomeLeader() {
	state = Leader
	log.Printf("Server %s became LEADER for term %d", serverID, currentTerm)
	go sendHeartbeats()
}

func sendHeartbeats() {
	for state == Leader {
		for _, peer := range peers {
			go sendHeartbeat(peer)
		}
		time.Sleep(1 * time.Second)
	}
}

func sendHeartbeat(peer string) {
	url := fmt.Sprintf("%s/heartbeat", peer)

	heartbeat := map[string]interface{}{
		"term":     currentTerm,
		"leaderId": serverID,
	}
	reqBody, _ := json.Marshal(heartbeat)

	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("Failed to send heartbeat to %s", peer)
		return
	}
	resp.Body.Close()
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var heartbeat map[string]interface{}
	json.NewDecoder(r.Body).Decode(&heartbeat)

	leaderTerm := int(heartbeat["term"].(float64))
	leaderID := heartbeat["leaderId"].(string)

	if leaderTerm >= currentTerm {
		state = Follower
		currentTerm = leaderTerm
		currentLeader = leaderID
		lastHeartbeat = time.Now()
		resetElectionTimeout()
		log.Printf("Received heartbeat from leader %s", leaderID)
	}
	w.WriteHeader(http.StatusOK)
}

func monitorLeader() {
	for {
		time.Sleep(100 * time.Millisecond)
		if time.Since(lastHeartbeat) > electionTimeout && state == Follower {
			log.Printf("Leader died. Starting election...")
			startElection()
		}
	}
}

func resetElectionTimeout() {
	electionTimeout = time.Duration(1000+rand.Intn(1000)) * time.Millisecond
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	serverID = port
	initPeers(port)

	resetElectionTimeout()
	lastHeartbeat = time.Now()
	go monitorLeader()

	fileName = "data_" + port + ".json"
	if err := loadFromFile(); err != nil {
		log.Printf("Error loading data: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /store/{key}", get)
	mux.HandleFunc("PUT /store/{key}", put)
	mux.HandleFunc("DELETE /store/{key}", del)
	mux.HandleFunc("POST /vote", handleVoteRequest)
	mux.HandleFunc("POST /heartbeat", handleHeartbeat)
	handler := logging(mux)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}
	log.Printf("listening on %s", port)
	log.Fatal(server.ListenAndServe())
}
