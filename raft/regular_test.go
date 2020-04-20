package raft

// import (
// 	"testing"
// 	"time"
// )

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
