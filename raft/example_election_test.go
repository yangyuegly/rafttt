package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Tests that nodes can successfully join a cluster and elect a leader.
func TestInit(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	followers, candidates, leaders := 0, 0, 0
	for i := 0; i < config.ClusterSize; i++ {
		node := cluster[i]
		switch node.State {
		case FollowerState:
			followers++
		case CandidateState:
			candidates++
		case LeaderState:
			leaders++
		}
	}

	if followers != config.ClusterSize-1 {
		t.Errorf("follower count mismatch, expected %v, got %v", config.ClusterSize-1, followers)
	}

	if candidates != 0 {
		t.Errorf("candidate count mismatch, expected %v, got %v", 0, candidates)
	}

	if leaders != 1 {
		t.Errorf("leader count mismatch, expected %v, got %v", 1, leaders)
	}
}

// Tests that if a leader is partitioned from its followers, a
// new leader is elected.
func TestNewElection(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 5

	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	oldLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// partition leader, triggering election
	oldTerm := oldLeader.GetCurrentTerm()
	oldLeader.NetworkPolicy.PauseWorld(true)

	// wait for new leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	// unpause old leader and wait for it to become a follower
	oldLeader.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WaitPeriod)

	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if oldLeader.Self.Id == newLeader.Self.Id {
		t.Errorf("leader did not change")
	}

	if newLeader.GetCurrentTerm() == oldTerm {
		t.Errorf("term did not change")
	}
}

func TestRequestVote_SentToLeader(t *testing.T) {

	node1, err := CreateNode(0, nil, DefaultConfig()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err := CreateNode(0, nil, DefaultConfig()) //receiver
	if err != nil {
		t.Errorf("send to leader")
	}
	//higher_term/higher_last_log_index/lower_last_log_term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	node1.LastLogIndex = 4
	node2.LastLogIndex = 3

	node1.LastLogTerm = 1
	node2.LastLogTerm = 2
	assert.False(r.processVoteRequest(node1.generateVoteRequest()))

}

func (r *RaftNode) generateVoteRequest() *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:         r.GetCurrentTerm(),
		Candidate:    r.Self,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	}
}

func TestRequestVote_SentToCandidate(t *testing.T) {

}

func TestRequestVote_SentToFollower(t *testing.T) {

}
