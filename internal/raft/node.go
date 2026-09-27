package raft

import (
	"context"
	"log"
	"sync"
	"time"

	movie "moviekv/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Node struct {
	mu sync.RWMutex

	ID string

	CurrentTerm int
	VotedFor    string

	Role     Role
	LeaderID string

	Peers map[string]string

	VotesReceived int

	// Raft log.
	Log *Log

	// Highest log entry known to be committed.
	CommitIndex int

	// Highest log entry already applied to the state machine.
	LastApplied int

	// Successful replication acknowledgements for each log index.
	ReplicationAcks map[int]map[string]bool
}

func NewNode(id string, peers map[string]string) *Node {
	return &Node{
		ID:            id,
		CurrentTerm:   0,
		VotedFor:      "",
		Role:          Follower,
		LeaderID:      "",
		Peers:         peers,
		VotesReceived: 0,
		Log:           NewLog(),

		CommitIndex: -1,
		LastApplied: -1,

		ReplicationAcks: make(map[int]map[string]bool),
	}
}

func (n *Node) GetRole() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Role
}

func (n *Node) GetTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.CurrentTerm
}

func (n *Node) GetVotesReceived() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.VotesReceived
}

func (n *Node) GetLastLogIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Log.LastIndex()
}

func (n *Node) GetLastLogTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Log.LastTerm()
}

func (n *Node) GetCommitIndex() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.CommitIndex
}

func (n *Node) GetLastApplied() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.LastApplied
}

func (n *Node) AppendLocalEntry(entry LogEntry) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	index := n.Log.Append(entry)

	// The leader counts its own log as the first acknowledgement.
	if n.Role == Leader {
		if _, exists := n.ReplicationAcks[index]; !exists {
			n.ReplicationAcks[index] = make(map[string]bool)
		}

		n.ReplicationAcks[index][n.ID] = true
	}

	log.Printf(
		"[%s] appended local log entry: index=%d term=%d command=%s key=%s request_id=%s",
		n.ID,
		index,
		entry.Term,
		entry.Command,
		entry.Key,
		entry.RequestID,
	)

	return index
}

func (n *Node) SetRole(role Role) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Role = role
}

func (n *Node) SetLeaderID(leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.LeaderID = leaderID
}

func (n *Node) BecomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.CurrentTerm++
	n.Role = Candidate
	n.VotedFor = n.ID
	n.LeaderID = ""
	n.VotesReceived = 1
}

func (n *Node) BecomeLeader() {
	n.mu.Lock()

	n.Role = Leader
	n.LeaderID = n.ID

	n.mu.Unlock()

	log.Printf(
		"[%s] became LEADER for term %d",
		n.ID,
		n.GetTerm(),
	)

	go n.heartbeatLoop()
}

func (n *Node) AddVote() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.VotesReceived++
}

func (n *Node) HandleRequestVote(
	term int,
	candidateID string,
) (int, bool) {

	n.mu.Lock()
	defer n.mu.Unlock()

	if term < n.CurrentTerm {
		return n.CurrentTerm, false
	}

	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.Role = Follower
		n.VotedFor = ""
		n.LeaderID = ""
		n.VotesReceived = 0
	}

	if n.VotedFor != "" && n.VotedFor != candidateID {
		return n.CurrentTerm, false
	}

	n.VotedFor = candidateID

	return n.CurrentTerm, true
}

func (n *Node) StartElection() {
	n.BecomeCandidate()

	term := n.GetTerm()

	log.Printf(
		"[%s] starting election for term %d",
		n.ID,
		term,
	)

	majority := (len(n.Peers)+1)/2 + 1

	for peerID, address := range n.Peers {

		if peerID == n.ID {
			continue
		}

		go func(
			peerID string,
			address string,
		) {

			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
			)

			if err != nil {
				log.Printf(
					"[%s] failed to connect to %s: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			defer conn.Close()

			client := movie.NewRaftServiceClient(conn)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				3*time.Second,
			)

			defer cancel()

			response, err := client.RequestVote(
				ctx,
				&movie.RequestVoteRequest{
					Term:         int32(term),
					CandidateId:  n.ID,
					LastLogIndex: int32(n.GetLastLogIndex()),
					LastLogTerm:  int32(n.GetLastLogTerm()),
				},
			)

			if err != nil {
				log.Printf(
					"[%s] RequestVote to %s failed: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			if response.GetVoteGranted() {

				n.AddVote()

				votes := n.GetVotesReceived()

				log.Printf(
					"[%s] received vote from %s (%d/%d)",
					n.ID,
					peerID,
					votes,
					majority,
				)

				if votes >= majority {
					n.BecomeLeader()
				}
			}

		}(peerID, address)
	}
}

// HandleAppendEntries accepts an optional leader commit index.
//
// The optional argument keeps this method compatible with the current
// server implementation while allowing the leader to send commit information.
func (n *Node) HandleAppendEntries(
	term int,
	leaderID string,
	entries []*movie.LogEntry,
	leaderCommit ...int,
) (int, bool) {

	n.mu.Lock()
	defer n.mu.Unlock()

	// Reject an old leader.
	if term < n.CurrentTerm {
		return n.CurrentTerm, false
	}

	// A newer term means this node must follow the new leader.
	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.VotedFor = ""
		n.LeaderID = ""
	}

	n.Role = Follower
	n.LeaderID = leaderID

	// Convert protobuf log entries into internal Raft log entries.
	for _, entry := range entries {

		internalEntry := LogEntry{
			Term:      int(entry.GetTerm()),
			Command:   entry.GetCommand(),
			Key:       entry.GetKey(),
			Value:     entry.GetValue(),
			RequestID: entry.GetRequestId(),
		}

		n.Log.Append(internalEntry)

		log.Printf(
			"[%s] replicated log entry: term=%d command=%s key=%s request_id=%s",
			n.ID,
			internalEntry.Term,
			internalEntry.Command,
			internalEntry.Key,
			internalEntry.RequestID,
		)
	}

	// Update follower commit index if the leader supplied one.
	if len(leaderCommit) > 0 {

		leaderCommitIndex := leaderCommit[0]

		if leaderCommitIndex > n.CommitIndex {

			lastLogIndex := n.Log.LastIndex()

			if leaderCommitIndex < lastLogIndex {
				n.CommitIndex = leaderCommitIndex
			} else {
				n.CommitIndex = lastLogIndex
			}

			log.Printf(
				"[%s] updated commitIndex=%d from leader=%s",
				n.ID,
				n.CommitIndex,
				leaderID,
			)
		}
	}

	return n.CurrentTerm, true
}

func (n *Node) AcknowledgeReplication(
	index int,
	nodeID string,
) bool {

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.ReplicationAcks[index]; !exists {
		n.ReplicationAcks[index] = make(map[string]bool)
	}

	n.ReplicationAcks[index][nodeID] = true

	ackCount := len(n.ReplicationAcks[index])

	// Peers contains all cluster nodes, including the current node.
	clusterSize := len(n.Peers)

	majority := clusterSize/2 + 1

	if ackCount >= majority && index > n.CommitIndex {

		n.CommitIndex = index

		log.Printf(
			"[%s] MAJORITY ACK reached for index=%d (%d/%d) -> commitIndex=%d",
			n.ID,
			index,
			ackCount,
			clusterSize,
			n.CommitIndex,
		)

		return true
	}

	return false
}

func (n *Node) ReplicateEntry(entry LogEntry) {

	n.mu.RLock()

	if n.Role != Leader {
		n.mu.RUnlock()

		log.Printf(
			"[%s] cannot replicate entry because node is not leader",
			n.ID,
		)

		return
	}

	term := n.CurrentTerm
	leaderID := n.ID
	index := n.Log.LastIndex()
	commitIndex := n.CommitIndex

	peers := make(map[string]string)

	for peerID, address := range n.Peers {
		peers[peerID] = address
	}

	n.mu.RUnlock()

	for peerID, address := range peers {

		if peerID == n.ID {
			continue
		}

		go func(
			peerID string,
			address string,
		) {

			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
			)

			if err != nil {
				log.Printf(
					"[%s] replication connection to %s failed: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			defer conn.Close()

			client := movie.NewRaftServiceClient(conn)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				3*time.Second,
			)

			defer cancel()

			response, err := client.AppendEntries(
				ctx,
				&movie.AppendEntriesRequest{
					Term:     int32(term),
					LeaderId: leaderID,
					Entries: []*movie.LogEntry{
						{
							Term:      int32(entry.Term),
							Command:   entry.Command,
							Key:       entry.Key,
							Value:     entry.Value,
							RequestId: entry.RequestID,
						},
					},
					LeaderCommit: int32(commitIndex),
				},
			)

			if err != nil {
				log.Printf(
					"[%s] log replication to %s failed: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			if !response.GetSuccess() {
				log.Printf(
					"[%s] follower %s rejected log entry index=%d",
					n.ID,
					peerID,
					index,
				)
				return
			}

			committed := n.AcknowledgeReplication(
				index,
				peerID,
			)

			log.Printf(
				"[%s] replication ACK from %s: index=%d committed=%t",
				n.ID,
				peerID,
				index,
				committed,
			)

		}(peerID, address)
	}
}

func (n *Node) SendHeartbeats() {

	n.mu.RLock()

	if n.Role != Leader {
		n.mu.RUnlock()
		return
	}

	term := n.CurrentTerm
	leaderID := n.ID
	commitIndex := n.CommitIndex

	peers := make(map[string]string)

	for peerID, address := range n.Peers {
		peers[peerID] = address
	}

	n.mu.RUnlock()

	for peerID, address := range peers {

		if peerID == n.ID {
			continue
		}

		go func(
			peerID string,
			address string,
		) {

			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
			)

			if err != nil {
				log.Printf(
					"[%s] heartbeat connection to %s failed: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			defer conn.Close()

			client := movie.NewRaftServiceClient(conn)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				2*time.Second,
			)

			defer cancel()

			response, err := client.AppendEntries(
				ctx,
				&movie.AppendEntriesRequest{
					Term:         int32(term),
					LeaderId:     leaderID,
					PrevLogIndex: int32(n.GetLastLogIndex()),
					PrevLogTerm:  int32(n.GetLastLogTerm()),
					LeaderCommit: int32(commitIndex),
				},
			)

			if err != nil {
				log.Printf(
					"[%s] heartbeat to %s failed: %v",
					n.ID,
					peerID,
					err,
				)
				return
			}

			log.Printf(
				"[%s] heartbeat → %s success=%t term=%d commitIndex=%d",
				n.ID,
				peerID,
				response.GetSuccess(),
				response.GetTerm(),
				commitIndex,
			)

		}(peerID, address)
	}
}

func (n *Node) heartbeatLoop() {

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {

		if n.GetRole() != Leader {
			return
		}

		n.SendHeartbeats()
	}
}

func (n *Node) ApplyCommittedEntries() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.CommitIndex <= n.LastApplied {
		return nil
	}

	start := n.LastApplied + 1
	end := n.CommitIndex

	applied := make([]LogEntry, 0, end-start+1)

	for index := start; index <= end; index++ {
		entry, ok := n.Log.Get(index)

		if !ok {
			continue
		}

		applied = append(applied, entry)

		log.Printf(
			"[%s] applying committed entry: index=%d term=%d command=%s key=%s request_id=%s",
			n.ID,
			index,
			entry.Term,
			entry.Command,
			entry.Key,
			entry.RequestID,
		)

		n.LastApplied = index
	}

	return applied
}

func (n *Node) WaitForCommit(index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		n.mu.RLock()
		committed := n.CommitIndex >= index
		n.mu.RUnlock()

		if committed {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}
