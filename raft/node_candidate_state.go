package raft

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *Node) doCandidate() stateFunction {
	r.Out("Transitioning to CandidateState")
	r.State = CandidateState

	r.setCurrentTerm(r.GetCurrentTerm() + 1)
	// vote for self
	votesCount := 1

	electionResults := make(chan bool, 1)
	fallback := make(chan bool, 1)

	timeout := randomTimeout(r.config.ElectionTimeout)
	go r.requestVotes(electionResults, fallback, r.GetCurrentTerm())
	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case appEn := <-r.appendEntries:
			resetTimeout, fallback := r.handleAppendEntries(appEn)
			if fallback {
				return r.doFollower
			}
		case reqVote := <-r.requestVote:
			fallback := r.handleCompetingRequestVote(reqVote)
			if fallback {
				return r.doFollower
			}
		case regCli := <-r.registerClient:
			regCli.reply <- RegisterClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				ClientId:   0,
				LeaderHint: r.Leader,
			}
		case cliReq := <-r.clientRequest:
			cliReq.reply <- ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				Response:   nil,
				LeaderHint: r.Leader,
			}
		case <-timeout:
			// start a new election

		case res := <-electionResults:
			if res {
				return r.doLeader
			} else {
				// start new election
			}
		case fall := <-fallback:
			if fall {
				return r.doFollower
			}
		}
	}

	return nil
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (r *Node) requestVotes(electionResults chan bool, fallback chan bool, currTerm uint64) {
	votes := 1
	peersLen := len(r.Peers)
	majority := r.config.ClusterSize/2 + 1

	votesChan := make(chan bool, peersLen)

	for _, peer := range r.Peers {
		go func() {
			reply, err := peer.RequestVoteRPC(r, &RequestVoteRequest{
				Term:         r.GetCurrentTerm(),
				Candidate:    r.Self,
				LastLogIndex: r.LastLogIndex(),
				LastLogTerm:  r.LastLogTerm(),
			})

			if err {
				r.Out("RequestVoteRPC to %v failed with %v", peer.Id, err)
				return
			}

			if reply.Term > r.GetCurrentTerm() {

			}
			votesChan <- reply.VoteGranted()
		}()

	}

	votesCount := 1
	// TODO: Continue this
	for i := 0; i < peersLen; i += 1 {
		res := <-votesChan
		if res {
			votesCount += 1
		}
	}

	return
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	return true
}
