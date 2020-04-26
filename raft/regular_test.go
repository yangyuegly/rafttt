package raft

import (
	"testing"
	"time"

	"github.com/brown-csci1380-s20/raft-yyang149-kboonyap/hashmachine"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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

	assert.Equal(t, node1.LastLogIndex(), uint64(4))
	assert.Equal(t, node2.LastLogIndex(), uint64(3))

	granted, _ := node2.processVoteRequest(node1.generateVoteRequest())
	assert.False(t, granted)

}
func TestRequestVote_Case2(t *testing.T) {
	node1, node2 := createNodeHelper(t)

	node1.setCurrentTerm(2)
	node2.setCurrentTerm(1)
	node1.StoreLog(&LogEntry{
		Index:  2,
		TermId: 2,
	})

	node2.StoreLog(&LogEntry{
		Index:  4,
		TermId: 1,
	})

	assert.Equal(t, node1.LastLogIndex(), uint64(2))
	assert.Equal(t, node2.LastLogIndex(), uint64(4))

	granted, term := node2.processVoteRequest(node1.generateVoteRequest())
	assert.True(t, granted)
	assert.Equal(t, term, uint64(2))

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
	fallback, voteGranted := node2.handleCompetingRequestVote(*msg)
	assert.False(t, fallback)
	assert.False(t, voteGranted)
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
		reply:   make(chan AppendEntriesReply, 1),
	}

	reset, _ := node1.handleAppendEntries(*appMsg)
	assert.False(t, reset)
	appReply := <-appMsg.reply
	assert.False(t, appReply.Success)

}

func TestAppendEntries_Case2(t *testing.T) {
	//equal_term_is_accepted_with_one_entry
	node1, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}

	node1.setCurrentTerm(1)
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
		PrevLogTerm:  1, //correct prevlog, should grant
		Entries:      entriesToAppend,
		LeaderCommit: 4,
	}
	appMsg := &AppendEntriesMsg{
		request: enRequest,
		reply:   make(chan AppendEntriesReply, 1),
	}

	reset, fallback := node1.handleAppendEntries(*appMsg)
	assert.True(t, reset)
	assert.True(t, fallback)

	appReply := <-appMsg.reply
	assert.True(t, appReply.Success)

}

func TestAppendEntries_Case3(t *testing.T) {
	//equal_term_is_accepted_with_higher_commit_index
	node1, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}

	node1.setCurrentTerm(1)
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
		Index:  3,
		TermId: 2,
	}
	node2.commitIndex = 4

	enRequest := &AppendEntriesRequest{
		Term:         node2.GetCurrentTerm(),
		Leader:       node2.Self,
		PrevLogIndex: 2,
		PrevLogTerm:  1, //correct prevlog, should grant
		Entries:      entriesToAppend,
		LeaderCommit: 4,
	}
	appMsg := &AppendEntriesMsg{
		request: enRequest,
		reply:   make(chan AppendEntriesReply, 1),
	}

	reset, fallback := node1.handleAppendEntries(*appMsg)

	assert.True(t, reset)
	assert.True(t, fallback)
	assert.Equal(t, uint64(3), node1.commitIndex)

	appReply := <-appMsg.reply
	assert.True(t, appReply.Success)

}

func TestAppendEntries_Case4(t *testing.T) {
	//equal_term_is_accepted_with_higher_commit_index
	node1, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}
	node2, err := CreateNode(OpenPort(0), nil, DefaultConfig(), new(hashmachine.HashMachine), NewMemoryStore()) //sender
	if err != nil {
		t.Errorf("send to leader")
	}

	node1.setCurrentTerm(1)
	node2.setCurrentTerm(8)
	node1.StoreLog(&LogEntry{
		Index:  2,
		TermId: 1,
	})

	node2.StoreLog(&LogEntry{
		Index:  3,
		TermId: 2,
	})

	entriesToAppend := make([]*LogEntry, 6)
	entriesToAppend[0] = &LogEntry{
		Index:  3,
		TermId: 2,
	}
	entriesToAppend[1] = &LogEntry{
		Index:  4,
		TermId: 2,
	}
	entriesToAppend[2] = &LogEntry{
		Index:  5,
		TermId: 2,
	}
	entriesToAppend[3] = &LogEntry{
		Index:  6,
		TermId: 2,
	}
	entriesToAppend[4] = &LogEntry{
		Index:  7,
		TermId: 2,
	}
	entriesToAppend[5] = &LogEntry{
		Index:  8,
		TermId: 2,
	}

	node2.commitIndex = 4

	enRequest := &AppendEntriesRequest{
		Term:         node2.GetCurrentTerm(),
		Leader:       node2.Self,
		PrevLogIndex: 2,
		PrevLogTerm:  1, //correct prevlog, should grant
		Entries:      entriesToAppend,
		LeaderCommit: 5,
	}
	appMsg := &AppendEntriesMsg{
		request: enRequest,
		reply:   make(chan AppendEntriesReply, 1),
	}

	reset, fallback := node1.handleAppendEntries(*appMsg)

	assert.True(t, reset)
	assert.True(t, fallback)
	assert.True(t, logsMatch(node2, []*Node{node1}))
	assert.Equal(t, uint64(5), node1.commitIndex)

	appReply := <-appMsg.reply
	assert.True(t, appReply.Success)

}

func TestInit_Follower(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ElectionTimeout = 10 * time.Second
	config.ClusterSize = 5

	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * WaitPeriod)

	for _, node := range cluster {
		if node.State != FollowerState {
			t.Errorf("Node %v is not follower (%v)", node.Self.Id, node.State)
		}
	}

}

func Test_Correct_Leader_Elected(t *testing.T) {
	suppressLoggers()

	cluster, err := createTestCluster([]int{7006, 7007, 7008, 7009, 7010})
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}
	cluster[0].setCurrentTerm(5)
	cluster[1].setCurrentTerm(4)
	cluster[4].setCurrentTerm(4)
	logEntry := &LogEntry{
		Index:  cluster[0].LastLogIndex() + 1,
		TermId: cluster[0].GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	cluster[0].StoreLog(logEntry)

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	if err != nil {
		t.Fatal(err)
	}
	realLeader, err := findLeader(cluster)
	realLeader.Out(realLeader.String())

	if cluster[0].State != LeaderState && cluster[1].State != LeaderState && cluster[4].State != LeaderState {
		t.Errorf("cluster state incorrect %v", cluster[0].State)
	}

	// add a new log entry to the new leader; SHOULD be replicated
	cluster[0].leaderMutex.Lock()
	logEntry = &LogEntry{
		Index:  cluster[0].LastLogIndex() + 1,
		TermId: cluster[0].GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{9, 2, 10, 12},
	}
	cluster[0].StoreLog(logEntry)
	cluster[0].leaderMutex.Unlock()
	time.Sleep(time.Second * WaitPeriod)
	if !logsMatch(cluster[0], cluster) {
		t.Errorf("logs incorrect")
	}
}

//Duplicate request
func TestClientInteraction_Cached_Request(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// First make sure we can register a client correctly
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("Counld not register client")
	}

	clientid := reply.ClientId

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	ClientReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     2,
		StateMachineCmd: hashmachine.HashChainAdd,
		Data:            []byte{},
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
	time.Sleep(2 * time.Second)

	//second round same request
	clientResult2, _ := leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult2.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
}

func TestClientInteraction_TwoNodeCluster(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 2
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("cannot find leader")
	}

	// First make sure we can register a client correctly
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Errorf("%v", reply.Status)
		t.Fatal("We don't have a leader yet")
	}
	logsMatch(leader, cluster)
}

//make a cluster
//find a leader
//manually make a request
//do a vote message

//send a request with the partition

func TestLeaderHandleCompetingVotes(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	followers := make([]*Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}
	req := &RequestVoteRequest{
		Term:         5,
		Candidate:    followers[0].Self,
		LastLogIndex: 4,
		LastLogTerm:  4,
	}

	reply := leader.RequestVote(req)

	assert.True(t, reply.VoteGranted)
}

func TestCandidateFallback(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.ClusterSize = 3
	config.ElectionTimeout = time.Millisecond * 150
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// cluster[0] should prevent other from getting elected
	cluster[0].config.ElectionTimeout = time.Second * 30
	cluster[0].setCurrentTerm(20)
	ticker := time.NewTicker(config.ElectionTimeout)
	defer ticker.Stop()

	cancelTick := make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				cluster[0].setCurrentTerm(cluster[0].GetCurrentTerm() + 2)
			case <-cancelTick:
				return
			}
		}
	}()

	defer func() {
		cancelTick <- true
	}()

	// reset timeout on cluster[0]
	cluster[0].AppendEntries(&AppendEntriesRequest{
		Term:         20,
		Leader:       cluster[0].Self,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	time.Sleep(time.Second * WaitPeriod)
	for _, node := range cluster {
		if node.State == LeaderState {
			t.Errorf("Found an elected leader! %v", node.Self.Id)
		}
	}
}
