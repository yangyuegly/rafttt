package raft

import (
	"testing"
	"time"
)

func Test_Multiple_Partition(t *testing.T) {
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

// func TestClientRequest_Leader_Partition(t *testing.T) {
// 	suppressLoggers()
// 	cluster, err := createTestCluster([]int{6001, 6002, 6003, 6004, 6005})
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
// 	if clientResultNew.Status != ClientStatus_OK {
// 		t.Fatal("New leader should be able to commit a client request")
// 	}
// 	// Make sure further request is correct processed
// 	ClientReq := ClientRequest{
// 		ClientId:        clientid,
// 		SequenceNum:     2,
// 		StateMachineCmd: hashmachine.HashChainAdd,
// 		Data:            []byte{},
// 	}
// 	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
// 	if clientResult.Status == ClientStatus_OK {
// 		t.Fatal("Old leader should not be able to commit a client request")
// 	}
// 	clientResultNew, _ = newLeader.ClientRequestCaller(context.Background(), &ClientReq)
// 	if clientResultNew.Status == ClientStatus_OK {
// 		t.Fatal("New leader should be able to commit a client request")
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

// func TestClientRegistration_Leader_Partition(t *testing.T) {
// 	suppressLoggers()
// 	cluster, err := createTestCluster([]int{7001, 7002, 7003, 7004, 7005})
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
// 	// rejoin the cluster
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
