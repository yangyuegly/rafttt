package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/brown-csci1380-s20/raft-yyang149-kboonyap/hashmachine"
	"golang.org/x/net/context"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestClientInteraction_Leader(t *testing.T) {
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
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestClientInteraction_Follower(t *testing.T) {
	suppressLoggers()
	config := DefaultConfig()
	// set the ElectionTimeout long enough to keep nodes in the state of follower
	config.ElectionTimeout = 60 * time.Second
	config.ClusterSize = 3
	config.HeartbeatTimeout = 500 * time.Millisecond
	cluster, err := CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	// make sure the client get the correct response while registering itself with a follower
	reply, _ := cluster[0].RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if reply.Status != ClientStatus_NOT_LEADER && reply.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to a follower")
	}

	// make sure the client get the correct response while sending a request to a follower
	req := ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := cluster[0].ClientRequestCaller(context.Background(), &req)
	if clientResult.Status != ClientStatus_NOT_LEADER && clientResult.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to a follower")
	}
}

func TestClientRegistration_Leader_Partition(t *testing.T) {
	suppressLoggers()
	cluster, err := createTestCluster([]int{5001, 5002, 5003, 5004, 5005})
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

	// partition into 2 clusters: one with leader and first follower; other with remaining 3 followers
	ff := followers[0]
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, false)

		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, false)
	}

	// allow a new leader to be elected in partition of 3 nodes
	time.Sleep(time.Second * WaitPeriod)
	newLeader, err := findLeader(followers)
	if err != nil {
		t.Fatal(err)
	}

	// check that old leader, which is cut off from new leader, still thinks it's leader
	if leader.State != LeaderState {
		t.Fatal(errors.New("leader should remain leader even when partitioned"))
	}

	if leader.GetCurrentTerm() >= newLeader.GetCurrentTerm() {
		t.Fatal(errors.New("new leader should have higher term"))
	}

	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

	if reply.Status == ClientStatus_OK {
		t.Fatal("Old leader should not be able to register client")
	}

	replyFromNew, _ := newLeader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	if replyFromNew.Status != ClientStatus_OK {
		t.Fatal("New leader should be able to register client")
	}
	// rejoin the cluster
	for _, follower := range followers[1:] {
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, true)
		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, true)

		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, true)
		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, true)
	}

	// wait for larger cluster to stabilize
	time.Sleep(time.Second * WaitPeriod)

	if !logsMatch(newLeader, cluster) {
		t.Errorf("logs incorrect")
	}

	// clientid := reply.ClientId
}

// func TestClientRequest_Leader_Partition(t *testing.T) {
// 	suppressLoggers()
// 	cluster, err := createTestCluster([]int{5001, 5002, 5003, 5004, 5005})
// 	defer cleanupCluster(cluster)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	time.Sleep(2 * time.Second)
// 	leader, err := findLeader(cluster)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	followers := make([]*Node, 0)
// 	for _, node := range cluster {
// 		if node != leader {
// 			followers = append(followers, node)
// 		}
// 	}

// 	// partition into 2 clusters: one with leader and first follower; other with remaining 3 followers
// 	ff := followers[0]
// 	for _, follower := range followers[1:] {
// 		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, false)
// 		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, false)

// 		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, false)
// 		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, false)
// 	}

// 	// allow a new leader to be elected in partition of 3 nodes
// 	time.Sleep(time.Second * WaitPeriod)
// 	newLeader, err := findLeader(followers)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// check that old leader, which is cut off from new leader, still thinks it's leader
// 	if leader.State != LeaderState {
// 		t.Fatal(errors.New("leader should remain leader even when partitioned"))
// 	}

// 	if leader.GetCurrentTerm() >= newLeader.GetCurrentTerm() {
// 		t.Fatal(errors.New("new leader should have higher term"))
// 	}

// 	reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})

// 	if reply.Status == ClientStatus_OK {
// 		t.Fatal("Old leader should not be able to register client")
// 	}

// 	replyFromNew, _ := newLeader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
// 	if replyFromNew.Status != ClientStatus_OK {
// 		t.Fatal("New leader should be able to register client")
// 	}

// 	// send client request during partition
// 	clientid := reply.ClientId

// 	// Hash initialization request
// 	initReq := ClientRequest{
// 		ClientId:        clientid,
// 		SequenceNum:     1,
// 		StateMachineCmd: hashmachine.HashChainInit,
// 		Data:            []byte("hello"),
// 	}
// 	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
// 	if clientResult.Status == ClientStatus_OK {
// 		t.Fatal("Old leader should not be able to commit a client request")
// 	}
// 	clientResultNew, _ := newLeader.ClientRequestCaller(context.Background(), &initReq)
// 	if clientResultNew.Status == ClientStatus_OK {
// 		t.Fatal("New leader should not be able to commit a client request")
// 	}
// 	// Make sure further request is correct processed
// 	ClientReq := ClientRequest{
// 		ClientId:        clientid,
// 		SequenceNum:     2,
// 		StateMachineCmd: hashmachine.HashChainAdd,
// 		Data:            []byte{},
// 	}
// 	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
// 	if clientResult.Status != ClientStatus_OK {
// 		t.Fatal("Leader failed to commit a client request")
// 	}

// 	for _, follower := range followers[1:] {
// 		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *ff.Self, true)
// 		follower.NetworkPolicy.RegisterPolicy(*follower.Self, *leader.Self, true)

// 		ff.NetworkPolicy.RegisterPolicy(*ff.Self, *follower.Self, true)
// 		leader.NetworkPolicy.RegisterPolicy(*leader.Self, *follower.Self, true)
// 	}

// 	// wait for larger cluster to stabilize
// 	time.Sleep(time.Second * WaitPeriod)

// 	if !logsMatch(newLeader, cluster) {
// 		t.Errorf("logs incorrect")
// 	}

// 	// clientid := reply.ClientId
// }
