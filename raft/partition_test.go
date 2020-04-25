package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/brown-csci1380-s20/raft-yyang149-kboonyap/hashmachine"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestThreeWayPartition(t *testing.T) {
	suppressLoggers()
	cluster, err := createTestCluster([]int{8001, 8002, 8003, 8004, 8005, 8006, 8007})
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

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
	// partition into 3 clusters: one with leader and first follower; one with 2 followers (1,2); one with another 2 followers (3,4,5);
	ff := followers[0]
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, false)
		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, false)
	}

	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].Self, *followers[3].Self, false)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].Self, *followers[4].Self, false)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].Self, *followers[3].Self, false)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].Self, *followers[4].Self, false)
	followers[1].NetworkPolicy.RegisterPolicy(*followers[1].Self, *followers[5].Self, false)
	followers[2].NetworkPolicy.RegisterPolicy(*followers[2].Self, *followers[5].Self, false)

	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].Self, *followers[1].Self, false)
	followers[3].NetworkPolicy.RegisterPolicy(*followers[3].Self, *followers[2].Self, false)
	followers[4].NetworkPolicy.RegisterPolicy(*followers[4].Self, *followers[1].Self, false)
	followers[4].NetworkPolicy.RegisterPolicy(*followers[4].Self, *followers[2].Self, false)
	followers[5].NetworkPolicy.RegisterPolicy(*followers[5].Self, *followers[1].Self, false)
	followers[5].NetworkPolicy.RegisterPolicy(*followers[5].Self, *followers[2].Self, false)

	// allow a new leader to be elected in partition of 3 nodes
	time.Sleep(time.Second * WaitPeriod)
	newLeader, err := findLeader(followers)
	if newLeader != nil {
		t.Error("There should be no leader?")
	}
	if err == nil {
		t.Error("There should be a no leader found error")
	}
}

func Test_One_Follower_Partitioned(t *testing.T) {
	suppressLoggers()

	cluster, err := createTestCluster([]int{6001, 6002, 6003, 6004, 6005})
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
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

	// partition into 2 clusters: one with one follower; another with the rest
	ff := followers[0]
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
	}
	leader.NetworkPolicy.RegisterPolicy(*leader.Self, *ff.Self, false)
	leader.NetworkPolicy.RegisterPolicy(*ff.Self, *leader.Self, false)

	//
	time.Sleep(time.Second * WaitPeriod)
	if ff.State == LeaderState {
		t.Fatal(errors.New("ff should not be a leader when partitioned"))
	}

	// add a new log entry to the new leader; SHOULD be replicated
	leader.leaderMutex.Lock()
	logEntry := &LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	leader.StoreLog(logEntry)
	leader.leaderMutex.Unlock()

	reply, _ := ff.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status == ClientStatus_OK {
		t.Fatal("One follower should not register client")
	}

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := ff.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status == ClientStatus_OK {
		t.Fatal("one follower should not commit a client request")
	}

	time.Sleep(time.Second * WaitPeriod)

	// rejoin the cluster
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, true)
		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, true)
	}
	leader.NetworkPolicy.RegisterPolicy(*leader.Self, *ff.Self, true)
	leader.NetworkPolicy.RegisterPolicy(*ff.Self, *leader.Self, true)

	// wait for larger cluster to stabilize
	time.Sleep(time.Second * WaitPeriod)

	if !logsMatch(ff, cluster) {
		t.Errorf("logs incorrect")
	}

}

func TestClientInteraction_Partition(t *testing.T) {
	suppressLoggers()
	cluster, err := createTestCluster([]int{7001, 7002, 7003, 7004, 7005})
	defer cleanupCluster(cluster)

	time.Sleep(WaitPeriod * time.Second)

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

	// partition into 2 clusters: one with one follower; another with the rest
	ff := followers[0]
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
	}

	//
	time.Sleep(time.Second * WaitPeriod)
	if ff.State == LeaderState {
		t.Fatal(errors.New("ff should not be a leader when partitioned"))
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

	reply2, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if reply2.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// rejoin the cluster
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, true)
		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, true)
	}

	// wait for larger cluster to stabilize
	time.Sleep(time.Second * WaitPeriod)

	if !logsMatch(ff, cluster) {
		t.Errorf("logs incorrect")
	}

}

func TestShutDown(t *testing.T) {
	config := DefaultConfig()
	config.ClusterSize = 5

	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(time.Second * WaitPeriod)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

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
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != ClientStatus_OK {
		t.Fatalf("Leader failed to commit a client request: %v", clientResult.Status)
	}

	port := cluster[0].port
	cluster[0].GracefulExit()
	time.Sleep(time.Second * WaitPeriod)
	leader, err = findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	cluster[0], err = CreateNode(OpenPort(port), leader.Self, config, new(hashmachine.HashMachine), NewMemoryStore())
	time.Sleep(time.Second * WaitPeriod)

	assert.True(t, logsMatch(leader, cluster))
}

func Test_CacheReply_WO_Processed(t *testing.T) {
	// suppressLoggers()
	config := DefaultConfig()
	config.ElectionTimeout = 1 * time.Second
	config.ClusterSize = 5

	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// add a new log entry to the leader; should be replicated
	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status != ClientStatus_OK {
		t.Fatal("should register client")
	}

	// oops, total network failure!
	for _, node := range cluster {
		node.NetworkPolicy.PauseWorld(true)
	}

	if leader.State != LeaderState {
		t.Fatal(errors.New("leader should remain leader even when partitioned"))
	}

	clientid := reply.ClientId

	// Hash initialization request
	initReq := ClientRequest{
		ClientId:        clientid,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}

	doneCh := make(chan bool, 2)
	go func() {
		clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
		if clientResult.Status != ClientStatus_OK {
			t.Fatal("Leader failed to commit a client request")
		}
		doneCh <- true
	}()

	//duplicate request
	go func() {
		clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
		if clientResult.Status != ClientStatus_OK {
			t.Fatal("Leader failed to commit a client request")
		}
		doneCh <- true
	}()

	time.Sleep(time.Millisecond * 500)

	// rejoin the cluster
	for _, node := range cluster {
		node.NetworkPolicy.PauseWorld(false)
	}

	// wait for larger cluster to stabilize
	time.Sleep(time.Second * WaitPeriod)

	for i := 0; i < 2; i++ {
		<-doneCh
	}

	// for _, node := range cluster {
	// 	node.Out("Log %v, %v, %v, %v", node.LastLogIndex(), node.LastLogTerm(), node.commitIndex, node.lastApplied)
	// }
	assert.True(t, logsMatch(leader, cluster))
}
