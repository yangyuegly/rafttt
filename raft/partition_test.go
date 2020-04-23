package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	cluster, err := createTestCluster([]int{6001, 6002, 6003, 6004, 6005})
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
	if reply.Status != ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}

	clientResult, _ = cluster[0].ClientRequestCaller(context.Background(), &req)
	if clientResult.Status != ClientStatus_REQ_FAILED && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a partitioned follower")
	}

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

func TestShutDown(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	config.InMemory = false

	cluster, _ := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	ports := make([]int, 5)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	ports[0] = leader.port

	followers := make([]*Node, 0)
	i := 1
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
			ports[i] = node.port
			i++
		}
	}
	port := followers[0].port
	path := followers[0].stableStore.Path()
	followers[0].GracefulExit()
	time.Sleep(time.Second * WaitPeriod)

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
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &req)

	time.Sleep(time.Second * WaitPeriod)
	crahsedNode, err := CreateNode(OpenPort(port), leader.Self, DefaultConfig(), new(hashmachine.HashMachine), NewBoltStore(path))
	time.Sleep(time.Second * WaitPeriod)
	assert.True(t, logsMatch(crahsedNode, cluster))
	for _, p := range ports {
		err := os.RemoveAll(filepath.Join(os.TempDir(), fmt.Sprintf("raft%v", p)))
		if err != nil {
			t.Fatal(err)
		}
	}
}
