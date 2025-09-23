package kv

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

type Store struct {
	data map[string]string
	mu   sync.RWMutex

	fileName string

	// Replicator for non-replication writes (?replicate=false)
	Replicator Replicator

	// LeaderURL returns the leader URL and true if known, otherwise false
	LeaderURL func() (string, bool)
}

// Replicator replicates key/value pairs to peers
type Replicator interface {
	Replicate(key, value string)
}

// New creates a Store with example data
func New(fileName string) *Store {
	return &Store{
		data: map[string]string{
			"user1": "a",
			"user2": "b",
			"user3": "c",
		},
		fileName: fileName,
	}
}

// Load reads the JSON file into the store if present
func (s *Store) Load() error {
	data, err := os.ReadFile(s.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(data, &s.data)
}

// Save writes the store to disk
func (s *Store) Save() error {
	s.mu.RLock()
	b, err := json.Marshal(s.data)
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(s.fileName, b, 0644)
}

// GetHandler handles GET /store/{key}
func (s *Store) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")

	s.mu.RLock() // Maps are not safe for concurrent reads and writes
	val, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "key not found"})
		return
	}
	json.NewEncoder(w).Encode(val)
}

// PutHandler handles PUT /store/{key}
// Redirect PUT to leader when this node is not the leader.
func (s *Store) PutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")

	isReplication := r.URL.Query().Get("replicate") == "false"

	if !isReplication {
		if s.LeaderURL != nil {
			if leader, ok := s.LeaderURL(); !ok {
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(map[string]string{"error": "no leader available"})
				return
			} else if leader != "" {
				w.Header().Set("Location", leader+r.URL.Path)
				w.WriteHeader(http.StatusTemporaryRedirect)
				json.NewEncoder(w).Encode(map[string]string{"error": "redirect to leader"})
				return
			}
		}
	}

	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	s.mu.Lock()
	s.data[key] = string(body)
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		log.Printf("Error saving: %v", err)
	}
	// Only the leader gets here (followers redirect to leader)
	if !isReplication && s.Replicator != nil {
		go s.Replicator.Replicate(key, string(body))
	}

	json.NewEncoder(w).Encode(map[string]string{"info": "key value saved"})
}

// DeleteHandler handles DELETE /store/{key}
func (s *Store) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	w.Header().Set("Content-Type", "application/json")

	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		log.Printf("error saving: %v", err)
	}
	json.NewEncoder(w).Encode("deleted")
}
