# Distributed Movie Ticket Booking Store using Raft Consensus

A distributed key-value store designed for a movie ticket booking workload.

## Project Overview

This project implements a distributed key-value store from scratch and progressively adds:

- LSM-tree based storage
- Write-Ahead Logging (WAL)
- In-memory MemTable
- SSTables
- Concurrent data structures
- gRPC client communication
- Request routing
- At-least-once semantics
- Raft consensus
- Kubernetes deployment
- Prometheus monitoring
- Grafana dashboards
- Fault-injection testing

## Movie Ticket Booking Domain

The store will manage:

- Movies
- Shows
- Screens
- Seats
- Seat availability
- Users
- Bookings

Example key:

`show:movie-101:screen-2:2026-10-01T19:00:seat:A10`

Example value:

`available`

## Development Milestones

### Weeks 1-2
- Project setup
- Go scaffold
- Git/GitHub
- Linux profiling baseline

### Weeks 3-4
- LSM-tree storage engine
- WAL
- MemTable
- SSTables
- RocksDB benchmarks

### Weeks 5-6
- Concurrent data structures
- Race detector
- Benchmarks

### Weeks 7-8
- gRPC
- Client routing
- At-least-once semantics

### Weeks 9-10
- Raft consensus
- Leader election
- Log replication
- Safety testing

### Weeks 11-12
- Kubernetes
- Helm
- Prometheus
- Grafana
- Fault injection
- Final performance evaluation

