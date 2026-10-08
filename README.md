
# Distributed Movie Ticket Booking Store using Raft Consensus

A distributed movie ticket booking key-value store built from the ground up using **Go**, **LSM-tree storage**, **gRPC**, **Raft consensus**, **Docker**, and **Kubernetes**.

The project explores how a distributed storage system can maintain **consistency, availability, durability, and fault tolerance** while handling concurrent requests, leader election, log replication, node failures, and recovery.

---

## Overview

The goal of this project is to build a small distributed storage system for a movie ticket booking workload.

Instead of relying on an existing database for the core storage and consensus logic, the system was developed incrementally from the storage layer upward:

```text
Storage Engine
      ↓
Concurrency
      ↓
gRPC Communication
      ↓
Booking & Request Handling
      ↓
Raft Consensus
      ↓
Persistent Raft Log
      ↓
Docker
      ↓
Kubernetes
      ↓
Fault-Tolerance Testing
```

Through this progression, the project demonstrates several important distributed-systems concepts:

- Key-value storage
- LSM-tree architecture
- Write-Ahead Logging
- MemTables
- SSTables
- Concurrent access
- gRPC communication
- Idempotent requests
- Leader election
- Raft consensus
- Replicated logs
- Majority-based commits
- Failure handling
- Node recovery
- Containerization
- Kubernetes deployment

---

## Project Objectives

The major objectives of the project were to:

1. Build a key-value storage engine from scratch.
2. Implement Write-Ahead Logging for durability.
3. Implement an in-memory MemTable.
4. Implement SSTable generation and lookup.
5. Support concurrent access to the storage layer.
6. Benchmark the custom storage components.
7. Compare the custom implementation with RocksDB.
8. Build a gRPC-based communication layer.
9. Implement movie ticket booking operations.
10. Support idempotent booking requests.
11. Implement Raft leader election.
12. Implement Raft log replication.
13. Commit operations using majority acknowledgement.
14. Persist Raft log entries using a separate Raft WAL.
15. Containerize the application using Docker.
16. Deploy a three-node Raft cluster using Kubernetes.
17. Test node failures and recovery.
18. Test cluster behavior when a majority is unavailable.

---

# Technology Stack

| Technology | Purpose |
|---|---|
| **Go** | Main programming language |
| **gRPC** | Client and node-to-node communication |
| **Protocol Buffers** | RPC and message definitions |
| **Go Modules** | Dependency management |
| **Git** | Version control |
| **GitHub** | Source-code repository |
| **RocksDB** | Storage benchmark reference |
| **Docker** | Application containerization |
| **Kind** | Local Kubernetes cluster |
| **Kubernetes** | Distributed deployment |
| **WSL2** | Linux development environment |

---

# High-Level Architecture

The system follows a layered distributed architecture:

```text
                     Movie Ticket Client
                            |
                            v
                    +---------------+
                    |   gRPC API    |
                    +---------------+
                            |
                            v
                    +---------------+
                    |    Request    |
                    | / Booking     |
                    |    Layer      |
                    +---------------+
                            |
                            v
                    +---------------+
                    | Raft Consensus|
                    +---------------+
                       /     |     \
                      /      |      \
                     v       v       v
                +---------+ +---------+ +---------+
                | Node 1  | | Node 2  | | Node 3  |
                |  Raft   | |  Raft   | |  Raft   |
                |  Store  | |  Store  | |  Store  |
                +---------+ +---------+ +---------+
                     \        |        /
                      \       |       /
                       +------+------+
                              |
                              v
                       +-------------+
                       | LSM Storage |
                       +-------------+
                              |
                 +------------+------------+
                 |            |            |
                 v            v            v
                WAL       MemTable      SSTable
```

Each Raft node contains its own storage layer and participates in replicated log consensus.

---

# Movie Ticket Booking Model

The distributed store is designed around a simple movie-ticket-booking domain.

The system can represent:

- Movies
- Shows
- Screens
- Seats
- Seat availability
- Users
- Bookings

For example, a seat can be represented using a key such as:

```text
seat:show-101:A10
```

with a value such as:

```text
available
```

After a successful booking:

```text
seat:show-101:A10
```

becomes:

```text
booked
```

A booking record can be represented as:

```text
booking:req-001
```

with a value such as:

```text
user-001|show-101|A10
```

---

# Project Structure

The repository is organized into separate components for the API, storage engine, Raft implementation, services, and deployment:

```text
distributed-movie-ticket-booking-store/
│
├── api/
│   ├── movie.proto
│   ├── movie.pb.go
│   └── movie_grpc.pb.go
│
├── cmd/
│   ├── client/
│   ├── cluster-check/
│   ├── kvm/
│   ├── peer-test/
│   ├── raft-log-test/
│   ├── raft-test/
│   └── server/
│
├── internal/
│   ├── lsm/
│   │   ├── memtable.go
│   │   ├── wal.go
│   │   ├── sstable.go
│   │   ├── rocksdb.go
│   │   └── benchmark files
│   │
│   ├── node/
│   │   └── config.go
│   │
│   ├── raft/
│   │   ├── log.go
│   │   ├── log_wal.go
│   │   └── node.go
│   │
│   └── store/
│       └── store.go
│
├── data/
├── Dockerfile
├── k8s-raft.yaml
├── go.mod
├── go.sum
└── README.md
```

---

# LSM-Tree Storage Engine

The storage layer follows the basic **Log-Structured Merge-tree (LSM-tree)** design.

```text
                PUT
                 |
                 v
             +-------+
             |  WAL  |
             +-------+
                 |
                 v
           +-----------+
           |  MemTable |
           +-----------+
                 |
                 | Full / Flush
                 v
           +-----------+
           |  SSTable  |
           +-----------+
```

The storage layer consists of three primary components:

1. **Write-Ahead Log (WAL)**
2. **MemTable**
3. **SSTable**

This provides a simple foundation for fast writes, persistent storage, and recovery.

---

# Write-Ahead Log

The Write-Ahead Log ensures that a change is recorded before it is applied to the in-memory storage.

The basic flow is:

```text
Client Request
      |
      v
  WAL Append
      |
      v
 MemTable Update
```

For example:

```text
seat:show-102:A10    booked
```

The WAL can be replayed during startup to reconstruct the MemTable, providing a basic recovery mechanism for the storage layer.

---

# MemTable

The MemTable is an in-memory key-value structure used for fast reads and writes.

It provides:

- Fast writes
- Fast reads
- Concurrent access
- Thread-safe updates

Conceptually:

```text
Put(key, value)
      |
      v
+----------------+
|    MemTable    |
|                |
|  key → value   |
+----------------+
```

A `Get(key)` operation retrieves the corresponding value from the MemTable.

The implementation uses synchronization primitives provided by Go's standard library.

---

# SSTable

SSTables provide persistent, sorted storage.

When MemTable data is flushed into an SSTable:

1. Keys are collected.
2. Keys are sorted.
3. Key-value pairs are written sequentially.
4. The resulting SSTable can be searched for stored keys.

Example:

```text
booking:001       user-001|show-101|A10
booking:002       user-002|show-101|B10
seat:show-101:A10 booked
seat:show-101:B10 available
```

The SSTable implementation supports lookup operations.

---

# Concurrency

Concurrency is an important part of the storage layer because multiple clients may access the system simultaneously.

The project includes concurrent tests for:

- Concurrent puts
- Concurrent gets
- Concurrent updates

The implementation was also tested using Go's race detector:

```bash
go test -race ./...
```

The race-detector tests completed successfully during development.

---

# RocksDB Benchmark

**RocksDB** was used as a reference storage implementation.

Native RocksDB was installed in the WSL environment and benchmarked through a Go wrapper.

Representative measurements from the development environment:

| Component | Operation | Result |
|---|---|---:|
| MemTable | Put | ~398 ns/op |
| MemTable | Get | ~214 ns/op |
| Concurrent MemTable | Put | ~252 ns/op |
| Concurrent MemTable | Get | ~88 ns/op |
| WAL | Append | ~469 ns/op |
| SSTable | Get | ~218 µs/op |
| SSTable | Write | ~2.31 ms/op |
| RocksDB | Put | ~3.49 µs/op |
| RocksDB | Get | ~1.42 µs/op |

These measurements are intended as reference values from the development environment.

The RocksDB operations are not directly equivalent to the individual MemTable, WAL, and SSTable microbenchmarks, so they should **not be interpreted as a strict apples-to-apples comparison**.

Performance may also vary depending on the CPU, operating system, filesystem, and runtime environment.

---

# gRPC Communication

The distributed system uses **gRPC** for communication between clients and Raft nodes.

Protocol Buffers define the RPC messages and services.

The main services are:

- `MovieTicketService`
- `NodeService`
- `RaftService`

---

# MovieTicketService

The movie-ticket API currently provides operations such as:

- `GetSeat`
- `BookSeat`

## GetSeat

`GetSeat` retrieves the current status of a seat.

Example request:

```text
show_id = show-102
seat_id = E5
```

Possible result:

```text
status = available
```

or:

```text
status = booked
```

---

# BookSeat

A booking request contains:

```text
show_id
seat_id
user_id
request_id
```

Example:

```text
show_id    = show-102
seat_id    = E5
user_id    = user-failure-test
request_id = req-failure-majority-001
```

The system generates a deterministic booking ID:

```text
BK-req-failure-majority-001
```

---

# Idempotent Booking

One of the important requirements of a distributed booking system is preventing duplicate bookings when a client retries a request.

Each booking request contains a unique:

```text
request_id
```

The system stores the result associated with that request ID.

If the same request is received again, the previously generated booking result is returned instead of creating another booking.

```text
Original Request
       |
       v
 Booking Created
       |
       v
 Booking ID Returned
       |
       |
       v
 Retry Same Request
       |
       v
 Existing Result Found
       |
       v
 Same Booking ID Returned
```

Example:

```text
Request ID:
req-failure-majority-001

First booking:
BK-req-failure-majority-001

Retry:
BK-req-failure-majority-001
```

This ensures that retrying the same request does not create duplicate bookings.

---

# Raft Consensus

The distributed store uses a **three-node Raft cluster**.

```text
                  +-----------+
                  |  Leader   |
                  +-----------+
                    /       \
                   /         \
                  v           v
          +-----------+   +-----------+
          | Follower  |   | Follower  |
          +-----------+   +-----------+
```

The cluster consists of:

```text
node1
node2
node3
```

At any point in time:

- One node acts as the **Leader**.
- The remaining nodes act as **Followers**.

The leader coordinates log replication and commits operations after receiving acknowledgement from a majority.

---

# Leader Election

The project implements Raft leader election using vote requests.

For a three-node cluster:

```text
Cluster size = 3
Majority     = 2
```

Therefore, a candidate needs at least two votes to become leader.

During testing, Node2 successfully became leader while Node3 was unavailable:

```text
[node2] starting election for term 1
[node2] received vote from node1 (2/2)
[node2] became LEADER for term 1
```

This demonstrated that the cluster can continue leader election when one node is unavailable.

---

# Raft Log Replication

A booking operation is represented as a Raft log entry.

Example:

```text
Term:
1

Command:
BOOK_SEAT

Key:
seat:show-102:E5

Request ID:
req-failure-majority-001
```

The leader replicates the entry to follower nodes:

```text
                 Client
                    |
                    v
                 Leader
                /      \
               v        v
          Follower 1  Follower 2
                \      /
                 \    /
              Majority ACK
                    |
                    v
                  Commit
                    |
                    v
               Apply to Store
```

---

# Majority-Based Commit

For a three-node cluster:

```text
3 nodes
   ↓
majority = 2
```

Therefore:

```text
2 available nodes → writes can commit
1 available node  → writes cannot commit
```

This behavior was explicitly tested using Kubernetes fault-injection scenarios.

---

# Persistent Raft Log

The Raft implementation includes a separate WAL for Raft log entries.

Example files:

```text
data/raft-node1.log
data/raft-node2.log
data/raft-node3.log
```

A Raft log entry is persisted before being added to the in-memory Raft log.

Example:

```text
1 BOOK_SEAT seat:show-102:D1 user-persistence|show-102|D1 req-persistence-001
```

When a node starts, the Raft WAL is replayed to reconstruct its log.

---

# Follower Recovery

When a follower falls behind, the leader sends missing log entries through `AppendEntries`.

Example recovery output:

```text
[node1] AppendEntries: leader=node2 term=2 entries=2 leaderCommit=0 success=true

[node3] AppendEntries: leader=node2 term=2 entries=2 leaderCommit=0 success=true
```

This demonstrates that a recovered follower can receive missing log entries from the current leader and catch up with the cluster.

---

# Docker

The application is containerized using Docker.

The Docker image contains:

- Go runtime
- Compiled movie server
- RocksDB native dependencies
- Application binary

Build the image with:

```bash
docker build -t movie-store:latest .
```

The resulting image is used by the Kubernetes deployment.

---

# Kubernetes Deployment

The project uses **Kind** to run a local Kubernetes cluster.

The Kind cluster contains:

```text
movie-store-control-plane
movie-store-worker
movie-store-worker2
```

The application is deployed as three Raft Pods:

```text
node1
node2
node3
```

Each Raft node has a corresponding Kubernetes Service.

---

# Kubernetes Services & Networking

The Raft nodes expose the following ports:

| Node | Port |
|---|---:|
| node1 | 50051 |
| node2 | 50052 |
| node3 | 50053 |

Kubernetes Services provide stable DNS names for peer communication.

The Raft peer configuration is:

```text
node1=node1:50051
node2=node2:50052
node3=node3:50053
```

This configuration is provided through the:

```text
RAFT_PEERS
```

environment variable.

The Kubernetes networking setup was verified using Raft heartbeat communication.

Example:

```text
[node2] heartbeat → node1 success=true term=1 entries=0 commitIndex=-1

[node2] heartbeat → node3 success=true term=1 entries=0 commitIndex=-1
```

This confirms that Kubernetes DNS and Service-based peer communication are functioning correctly.

---

# Fault-Tolerance Testing

One of the major goals of the project was to verify how the system behaves when Raft nodes fail.

Fault injection was performed by deleting Kubernetes Pods.

The following scenarios were tested.

---

## Test 1 — Node Failure

Initial cluster:

```text
node1 → Running
node2 → Running
node3 → Running
```

Node3 was deleted:

```bash
kubectl delete pod node3
```

Result:

```text
node1 → Running
node2 → Running
node3 → Down
```

The remaining two nodes maintained communication.

---

## Test 2 — Leader Election After Node Failure

With Node3 unavailable, Node2 successfully became leader:

```text
[node2] starting election for term 1
[node2] received vote from node1 (2/2)
[node2] became LEADER for term 1
```

This demonstrates majority-based leader election.

---

## Test 3 — Booking With One Node Failed

With Node3 down, a booking was submitted for:

```text
Show:
show-102

Seat:
E5

Request:
req-failure-majority-001
```

Before booking:

```text
status=available
```

Booking result:

```text
booking=BK-req-failure-majority-001
status=confirmed
message=Seat booked successfully
```

After booking:

```text
status=booked
```

The same request was submitted again.

Retry result:

```text
booking=BK-req-failure-majority-001
status=confirmed
```

This demonstrates:

1. Majority-based availability.
2. Successful booking with one node unavailable.
3. Idempotent retry handling.

---

# Test 4 — No Majority

Node1 was then deleted as well.

The cluster became:

```text
node1 → Down
node2 → Running
node3 → Down
```

Only one node remained.

A new booking was attempted:

```text
Show:
show-102

Seat:
F7

Request:
req-no-majority-001
```

Before booking:

```text
status=available
```

The booking failed:

```text
BookSeat failed:
rpc error: code = DeadlineExceeded
```

This demonstrates that a single surviving node cannot commit a booking because the three-node Raft cluster requires a majority of two nodes.

---

# Test 5 — Node Recovery

Node1 and Node3 were recreated using:

```bash
kubectl apply -f k8s-raft.yaml
```

After recovery:

```text
node1 → Running
node2 → Running
node3 → Running
```

The recovered nodes successfully received missing log entries from Node2.

Node1:

```text
[node1] AppendEntries:
leader=node2
term=2
entries=2
success=true
```

Node3:

```text
[node3] AppendEntries:
leader=node2
term=2
entries=2
success=true
```

The leader subsequently reported successful communication:

```text
[node2] heartbeat → node1 success=true term=2 entries=2 commitIndex=0

[node2] heartbeat → node3 success=true term=2 entries=2 commitIndex=0
```

This demonstrates Raft-based node recovery and log re-replication.

---

# Complete Fault-Tolerance Flow

The complete tested failure-and-recovery scenario can be summarized as:

```text
              3 Nodes
                 |
                 v
        +----------------+
        | node1 node2    |
        | node3          |
        +----------------+
                 |
              Node3 fails
                 |
                 v
        +----------------+
        | node1 node2    |
        +----------------+
                 |
            Majority = 2
                 |
                 v
          Booking succeeds
                 |
                 v
              Node1 fails
                 |
                 v
              Node2 only
                 |
             No majority
                 |
                 v
           Booking fails
                 |
                 v
          Recover node1/node3
                 |
                 v
              3 Nodes
                 |
                 v
          Raft log recovery
                 |
                 v
        Cluster operational
```

---

# Testing Commands

### Run all Go tests

```bash
go test ./...
```

### Run the race detector

```bash
go test -race ./...
```

### Build the server

```bash
go build ./cmd/server
```

### Build the Docker image

```bash
docker build -t movie-store:latest .
```

### Check Kubernetes nodes

```bash
kubectl get nodes
```

### Check Pods

```bash
kubectl get pods -o wide
```

### Check Services

```bash
kubectl get services
```

### Check Endpoints

```bash
kubectl get endpoints
```

### View node logs

```bash
kubectl logs node1
kubectl logs node2
kubectl logs node3
```

---

# Running the Kubernetes Deployment

## 1. Create the Kind Cluster

```bash
kind create cluster --name movie-store --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
EOF
```

## 2. Load the Docker Image

```bash
kind load docker-image movie-store:latest --name movie-store
```

## 3. Deploy the Raft Cluster

```bash
kubectl apply -f k8s-raft.yaml
```

## 4. Verify the Pods

```bash
kubectl get pods -o wide
```

Expected:

```text
node1   1/1   Running
node2   1/1   Running
node3   1/1   Running
```

---

# Port Forwarding for Local Testing

The Kubernetes Pods can be accessed locally using port forwarding.

### Node1

```bash
kubectl port-forward pod/node1 50051:50051
```

### Node2

```bash
kubectl port-forward pod/node2 50052:50052
```

### Node3

```bash
kubectl port-forward pod/node3 50053:50053
```

The gRPC client can then communicate with the selected node.

---

# Git & Version Control

The project is maintained using Git and GitHub.

Repository:

```text
https://github.com/sivateja215/distributed-movie-ticket-booking-store
```

The project was developed through incremental milestones, including:

- Storage implementation
- RocksDB benchmarking
- gRPC booking
- Raft functionality
- Raft persistence
- Kubernetes deployment
- Fault-tolerance testing

---

# Development Milestones

## Weeks 1–2 — Project Setup

Completed:

- Project topic finalized
- Go project scaffold
- Git repository
- GitHub repository
- Linux/WSL development environment

## Weeks 3–4 — Storage Layer

Completed:

- MemTable
- WAL
- SSTable
- Storage tests
- RocksDB benchmark reference

## Weeks 5–6 — Concurrency

Completed:

- Concurrent MemTable access
- Concurrent update testing
- Race detector testing
- Go benchmarks

## Weeks 7–8 — Networking

Completed:

- Protocol Buffers
- gRPC server
- gRPC client
- Node service
- Movie ticket booking service
- Request idempotency
- Peer communication

## Weeks 9–10 — Raft

Completed:

- Raft node
- Leader election
- Majority voting
- Log entries
- Log replication
- Majority acknowledgement
- Commit index
- Applying committed entries
- Persistent Raft WAL
- Follower recovery
- Leader failure testing

## Weeks 11–12 — Deployment & Fault Tolerance

Completed:

- Docker image
- Kind Kubernetes cluster
- Three Raft Pods
- Kubernetes Services
- Kubernetes DNS peer communication
- Node failure testing
- Majority-write testing
- No-majority testing
- Node recovery
- Raft log re-replication

---

# Performance Results

Representative benchmark results collected during development:

| Benchmark | Result |
|---|---:|
| `BenchmarkMemTablePut-16` | ~398 ns/op |
| `BenchmarkMemTableGet-16` | ~214 ns/op |
| `BenchmarkMemTableConcurrentPut-16` | ~252 ns/op |
| `BenchmarkMemTableConcurrentGet-16` | ~88 ns/op |
| `BenchmarkWALAppend-16` | ~469 ns/op |
| `BenchmarkSSTableGet-16` | ~218 µs/op |
| `BenchmarkSSTableWrite-16` | ~2.31 ms/op |
| `BenchmarkRocksDBPut-16` | ~3.49 µs/op |
| `BenchmarkRocksDBGet-16` | ~1.42 µs/op |

These results were measured during development and can vary depending on the hardware, operating system, filesystem, and runtime environment.

---

# Reliability Properties Demonstrated

The project demonstrates several fundamental distributed-system properties.

### Majority-Based Commit

Writes require acknowledgement from a majority of Raft nodes.

```text
3-node cluster
      |
      v
2 acknowledgements
      |
      v
    Commit
```

### Leader Election

If the current leader becomes unavailable, another node can become leader after obtaining a majority of votes.

### Idempotency

Repeated requests with the same request ID return the same booking result rather than creating duplicate bookings.

### Replicated Logs

Raft log entries are replicated from the leader to follower nodes.

### Failure Detection

Unavailable peers are detected through failed gRPC communication and Raft heartbeat failures.

### Recovery

Recovered nodes can receive missing log entries from the current leader.

---

# Known Limitations

This project is an **academic/prototype distributed storage system**, not a production-ready database.

There are several areas that could be improved.

## 1. Kubernetes Storage

The current Kubernetes Pods use container-local storage for the Raft WAL.

Deleting a Pod removes its local WAL.

The node can subsequently recover its replicated log from the leader through Raft.

A production deployment should use:

```text
StatefulSet
    +
PersistentVolume
    +
PersistentVolumeClaim
```

to provide durable Raft storage.

## 2. Simplified Raft Implementation

The current Raft implementation is intentionally simplified.

Production-level improvements could include:

- Complete `nextIndex` management
- Complete `matchIndex` management
- Advanced log-conflict handling
- More sophisticated election timeout management
- Persistent Raft state improvements

## 3. Kubernetes Deployment

The current deployment uses explicitly defined Pods and Services.

A production deployment would preferably use a Kubernetes StatefulSet.

## 4. Monitoring

Prometheus and Grafana are not currently included in the deployed implementation.

They are potential future enhancements.

---

# Future Improvements

Several improvements can be added to move the project closer to a production-style distributed system.

## Kubernetes StatefulSet

Replace manually defined Pods with a StatefulSet:

```text
movie-store-0
movie-store-1
movie-store-2
```

This would provide stable identities and storage for each Raft node.

## Persistent Volumes

Use PersistentVolumes and PersistentVolumeClaims for durable Raft WAL storage:

```text
Node
 |
 +-- PersistentVolume
        |
        +-- Raft WAL
```

## Prometheus Metrics

Add metrics such as:

- Current term
- Leader status
- Commit index
- Log length
- Request count
- Booking latency
- Failed RPC count

## Grafana Dashboards

Create dashboards for:

- Raft health
- Node availability
- Booking throughput
- Request latency
- Replication status

## Improved Raft

Future versions can implement:

- Proper randomized election timeouts
- Full `nextIndex`
- Full `matchIndex`
- Log conflict resolution
- Snapshotting
- Membership changes
- Improved persistent Raft state

## Automated Fault Injection

Fault-tolerance testing can be automated as:

```text
Kill Node
    ↓
Submit Booking
    ↓
Verify Majority
    ↓
Recover Node
    ↓
Verify Consistency
```

This would make it easier to repeatedly validate cluster behavior under different failure scenarios.

---

# Conclusion

The **Distributed Movie Ticket Booking Store** demonstrates how a distributed storage system can be built incrementally from the ground up.

The project combines:

```text
LSM Storage
    +
WAL
    +
MemTable
    +
SSTables
    +
Concurrency
    +
gRPC
    +
Idempotent Booking
    +
Raft Consensus
    +
Docker
    +
Kubernetes
```

The system was tested under multiple failure scenarios.

When one node was unavailable, the remaining two nodes were able to maintain a majority, elect a leader, and successfully commit a booking.

When two nodes were unavailable, the remaining single node could not commit a booking because a Raft majority was no longer available.

After the failed nodes were recreated, the recovered nodes successfully received missing log entries from the leader and rejoined the cluster.

Overall, the project demonstrates the fundamental principles behind a **fault-tolerant distributed movie ticket booking store using Raft consensus**, while also providing a practical foundation for exploring storage engines, distributed consensus, containerization, Kubernetes networking, and failure recovery.

---

## Repository

**GitHub:**  
https://github.com/sivateja215/distributed-movie-ticket-booking-store