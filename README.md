### raftkv-go

HTTP key-value store in Go with a small Raft implementation
A learning project

#### File structure
```
├── main.go             # HTTP server
├── kv/
│   └── kv.go           # Key-value store
├── raft/
│   └── raft.go         # Raft consensus
├── data_*.json         # Data storage files
└── README.md
```

#### Quick start

1. Run a node on a port (default 8080):

```bash
go run main.go 8080
```

2. Start nodes on different ports in terminals (example):

```bash
go run main.go 8000
go run main.go 8080
go run main.go 8081
```

#### API usage

##### GET /store/{key} 
`curl localhost:{peer}/store/{key}`
Get a value

##### PUT /store/{key}
`curl -X PUT localhost:{peer}/store/{key}`
- If the node is a follower: redirects to the current leader
- If the node is the leader: processes the request and replicates to followers

##### DELETE /store/{key}
`curl -X DELETE localhost:{port}/store/{key}`
Delete a key

##### POST /vote
Internal Raft vote request

##### POST /heartbeat
Internal Raft heartbeat

#### Configuration

##### Default ports
- localhost:8000
- localhost:8080
- localhost:8081

##### Data files
Each node save data to data_{port}.json, e.g. localhost:8080 saves to data_8080.json
