package raft

import (
	"testing"
	"time"

	"github.com/brown-csci1380-s20/raft-yyang149-kboonyap/hashmachine"
	"github.com/stretchr/testify/assert"
)

func TestRequestVote(t *testing.T) {

	node1, node2 := createNodeHelper(t)

	node2.StoreLog(&LogEntry{
		Index:  3,
		TermId: 2,
	})

	granted, _ := node2.processVoteRequest(node1.generateVoteRequest())
	assert.False(t, granted)

}

func createNodeHelper(t *testing.T) (node1 *Node, node2 *Node) {
	node1, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err = CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	return node1, node2
}

func TestRequestVote_Case1(t *testing.T) {

	node1, node2 := createNodeHelper(t)

	//higher_term/higher_last_log_index/lower_last_log_term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	node1.StoreLog(&LogEntry{
		Index:  4,
		TermId: 1,
	})

	node2.StoreLog(&LogEntry{
		Index:  3,
		TermId: 2,
	})

	granted, _ := node2.processVoteRequest(node1.generateVoteRequest())
	assert.False(t, granted)

}
func TestRequestVote_Case2(t *testing.T) {

	node1, node2 := createNodeHelper(t)

	//higher_term/lower_last_log_index/higher_last_log_term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	node1.StoreLog(&LogEntry{
		Index:  2,
		TermId: 12,
	})

	node2.StoreLog(&LogEntry{
		Index:  4,
		TermId: 1,
	})

	granted, _ := node2.processVoteRequest(node1.generateVoteRequest())
	assert.False(t, granted)

}

func TestHandleCompetingVotes(t *testing.T) {

	node1, node2 := createNodeHelper(t)

	//equal_term/lower_last_log_index/higher_last_log_term
	node1.setCurrentTerm(1)
	node2.setCurrentTerm(1)
	node1.StoreLog(&LogEntry{
		Index:  1,
		TermId: 1,
	})

	node2.StoreLog(&LogEntry{
		Index:  1,
		TermId: 1,
	})
	reply := make(chan RequestVoteReply)
	msg := &RequestVoteMsg{node1.generateVoteRequest(), reply}

	// node2.State = CandidateState
	fallback := node2.handleCompetingRequestVote(*msg)
	assert.False(t, fallback)

}

func (r *Node) generateVoteRequest() *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:         r.GetCurrentTerm(),
		Candidate:    r.Self,
		LastLogIndex: r.LastLogIndex(),
		LastLogTerm:  r.LastLogTerm(),
	}
}

func TestAppendEntries(t *testing.T) {
	//
	node1, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	//higher_term/higher_last_log_index/lower_last_log_term
	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	node1.StoreLog(&LogEntry{
		Index:  2,
		TermId: 1,
	})

	node2.StoreLog(&LogEntry{
		Index:  3,
		TermId: 2,
	})

	entriesToAppend := make([]*LogEntry, 1)
	entriesToAppend[0] = &LogEntry{
		Index:  4,
		TermId: 2,
	}
	node2.commitIndex = 4

	enRequest := &AppendEntriesRequest{
		Term:         node2.GetCurrentTerm(),
		Leader:       node2.Self,
		PrevLogIndex: 2,
		PrevLogTerm:  3, //incorrect prevlog, should reject
		Entries:      entriesToAppend,
		LeaderCommit: 4,
	}
	appMsg := &AppendEntriesMsg{
		request: enRequest,
	}
	reset, _ := node1.handleAppendEntries(*appMsg)
	assert.False(t, reset)
	appReply := <-appMsg.reply
	assert.False(t, appReply.Success)

}

// func TestInit_Follower(t *testing.T) {
// 	suppressLoggers()
// 	config := DefaultConfig()
// 	config.ElectionTimeout = 100 * time.Second //create a large election timeout

// 	cluster, err := CreateLocalCluster(config)
// 	defer cleanupCluster(cluster)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// wait for a leader to be elected
// 	time.Sleep(time.Second * WaitPeriod)
// 	followerNode:= CreateNode()

// 	followers, candidates, leaders := 0, 0, 0
// 	for i := 0; i < config.ClusterSize; i++ {
// 		node := cluster[i]
// 		switch node.State {
// 		case FollowerState:
// 			followers++
// 		case CandidateState:
// 			candidates++
// 		case LeaderState:
// 			leaders++
// 		}
// 	}

// 	if followers != config.ClusterSize-1 {
// 		t.Errorf("follower count mismatch, expected %v, got %v", config.ClusterSize-1, followers)
// 	}

// 	if candidates != 0 {
// 		t.Errorf("candidate count mismatch, expected %v, got %v", 0, candidates)
// 	}

// 	if leaders != 1 {
// 		t.Errorf("leader count mismatch, expected %v, got %v", 1, leaders)
// 	}
// }

func TestInit_Follower(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ElectionTimeout = 50 * time.Second
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

	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if oldLeader.Self.Id != newLeader.Self.Id {
		t.Errorf("leader did not change")
	}

}
