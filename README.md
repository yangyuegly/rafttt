# Raft

# regular_test:

- TestRequestVote : check if vote rejected for lower term
- TestRequestVote_Case1 : check if vote rejected for higher_term/higher_last_log_index/lower_last_log_term
- TestRequestVote_Case2: check if vote granted correctly to higher term/higher last log term id,
- TestHandleCompetingVotes: check if competing votes handled correctly
- TestAppendEntries: check if appendEntries requests fails with incorrect plt
- TestAppendEntries_Case2: check if appenEntries request succeeds with
- TestAppendEntries_Case3: check if correct commitIndex is set after accepting appendEntries Request
- TestAppendEntries_Case4: check appending multiple entries logs match
- TestInit_Follower: check if node stays a follower with long election timeout
- Test_Correct_Leader_Elected: check if the correct leader is elected and logs replicated

# partition_test:

- TestThreeWayPartition: check if a new leader is successfully elected in a three way partition and if logs replicated after parition healed.
- Test_One_Follower_Partitioned: make sure that no leader is elected in a partition with one node (not majority)
- TestClientInteraction_Partition: test client interaction with partitioned cluster; make sure client cannot successfuly send a request to a partitioned follower, and logs are replicated once the partition healed.
- TestClientInteraction_Cached_Request: duplicate client request to leader
- TestShutDown:Make sure that follower can shutdown and recover without disturbing the cluster.
