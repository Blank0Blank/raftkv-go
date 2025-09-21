package raft

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

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Node struct {
	mu sync.Mutex

	serverID string
	peers    map[string]string

	currentTerm int
	state       ServerState
	votedFor    string

	electionTimeout time.Duration
	lastHeartbeat   time.Time
}

func NewNode(serverID string, peers map[string]string) *Node {
	n := &Node{
		serverID: serverID,
		peers:    peers,
		state:    Follower,
	}
	n.resetElectionTimeout()
	n.lastHeartbeat = time.Now()
	return n
}

func (n *Node) Start() {
	go n.monitorLeader()
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.serverID
	}
	return ""
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.serverID
	votes := 1
	log.Printf("Server %s starting election for term %d", n.serverID, n.currentTerm)
	peers := make([]string, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	var m sync.Mutex
	for _, peer := range peers {
		wg.Add(1)
		go func(peerURL string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/vote", peerURL)
			voteReq := map[string]interface{}{
				"term":        n.currentTerm,
				"candidateId": n.serverID,
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
				m.Lock()
				votes++
				log.Printf("Got vote from %s, total votes: %d", peerURL, votes)
				if votes > len(n.peers)/2 {
					n.becomeLeader()
				}
				m.Unlock()
			}
		}(peer)
	}
	wg.Wait()
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	n.state = Leader
	log.Printf("Server %s became LEADER for term %d", n.serverID, n.currentTerm)
	n.mu.Unlock()
	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		peers := make([]string, 0, len(n.peers))
		for _, p := range n.peers {
			peers = append(peers, p)
		}
		n.mu.Unlock()

		for _, peer := range peers {
			go n.sendHeartbeat(peer)
		}
		time.Sleep(1 * time.Second)
	}
}

func (n *Node) sendHeartbeat(peer string) {
	url := fmt.Sprintf("%s/heartbeat", peer)
	heartbeat := map[string]interface{}{
		"term":     n.currentTerm,
		"leaderId": n.serverID,
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

func (n *Node) HandleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var voteReq map[string]interface{}
	json.NewDecoder(r.Body).Decode(&voteReq)
	candidateTerm := int(voteReq["term"].(float64))
	candidateID := voteReq["candidateId"].(string)

	voteGranted := false
	n.mu.Lock()
	if n.votedFor == "" || candidateTerm > n.currentTerm {
		n.votedFor = candidateID
		n.currentTerm = candidateTerm
		voteGranted = true
		log.Printf("Granted vote to %s for term %d", candidateID, candidateTerm)
	}
	n.mu.Unlock()
	response := map[string]interface{}{
		"voteGranted": voteGranted,
		"term":        n.currentTerm,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *Node) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var heartbeat map[string]interface{}
	json.NewDecoder(r.Body).Decode(&heartbeat)
	leaderTerm := int(heartbeat["term"].(float64))
	leaderID := heartbeat["leaderId"].(string)

	n.mu.Lock()
	if leaderTerm >= n.currentTerm {
		n.state = Follower
		n.currentTerm = leaderTerm
		n.votedFor = leaderID
		n.lastHeartbeat = time.Now()
		n.resetElectionTimeout()
		log.Printf("Received heartbeat from leader %s", leaderID)
	}
	n.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (n *Node) monitorLeader() {
	for {
		time.Sleep(100 * time.Millisecond)
		n.mu.Lock()
		if time.Since(n.lastHeartbeat) > n.electionTimeout && n.state == Follower {
			log.Printf("Leader died. Starting election...")
			n.mu.Unlock()
			n.startElection()
			continue
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionTimeout() {
	n.electionTimeout = time.Duration(1000+rand.Intn(1000)) * time.Millisecond
}
