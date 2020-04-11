package raft

// doCandidate implements the logic for a Raft node in the candidate state.
func (r *Node) doCandidate() stateFunction {
	r.Out("Transitioning to CandidateState")
	r.State = CandidateState

	r.setCurrentTerm(r.GetCurrentTerm() + 1)
	r.setVotedFor(r.Self.Id)

	electionResults := make(chan bool, 1)
	fallback := make(chan bool, len(r.Peers))

	timeout := randomTimeout(r.config.ElectionTimeout)
	go r.requestVotes(electionResults, fallback, r.GetCurrentTerm())

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case appEn := <-r.appendEntries:
			_, fallback := r.handleAppendEntries(appEn)
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
			r.setCurrentTerm(r.GetCurrentTerm() + 1)
			r.setVotedFor(r.Self.Id)
			electionResults = make(chan bool, 1)
			fallback = make(chan bool, 1)

			timeout = randomTimeout(r.config.ElectionTimeout)
			go r.requestVotes(electionResults, fallback, r.GetCurrentTerm())
		case res := <-electionResults:
			if res {
				return r.doLeader
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
	peersLen := len(r.Peers)
	majority := r.config.ClusterSize/2 + 1

	votesChan := make(chan bool, peersLen)

	for _, peer := range r.Peers {
		go func() {
			reply, err := peer.RequestVoteRPC(r, &RequestVoteRequest{
				Term:         currTerm,
				Candidate:    r.Self,
				LastLogIndex: r.LastLogIndex(),
				LastLogTerm:  r.LastLogTerm(),
			})

			if err != nil {
				r.Out("RequestVoteRPC to %v failed with %v", peer.Id, err)
				votesChan <- false
				return
			}

			if reply.Term > currTerm {
				// we're out of date, fallback!
				r.setCurrentTerm(reply.Term)
				r.setVotedFor("")
				fallback <- true
			}

			votesChan <- reply.VoteGranted
		}()
	}

	votesCount := 0

	for i := 0; i < peersLen; i++ {
		res := <-votesChan
		if res {
			votesCount++
			if votesCount >= majority {
				electionResults <- true
				return
			}
		}
	}

	electionResults <- (votesCount >= majority)
	return
}

// handleCompetingRequestVote handles an incoming vote request when the current
// node is in the candidate or leader state. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (r *Node) handleCompetingRequestVote(msg RequestVoteMsg) (fallback bool) {

	if msg.request.Term > r.GetCurrentTerm() {
		r.setCurrentTerm(msg.request.Term)
		msg.reply <- RequestVoteReply{
			Term:        r.GetCurrentTerm(),
			VoteGranted: true,
		}
		r.setVotedFor(msg.request.Candidate.Id)

		return true
	} else if msg.request.Term == r.GetCurrentTerm() {
		msg.reply <- RequestVoteReply{
			Term:        r.GetCurrentTerm(),
			VoteGranted: (r.GetVotedFor() == msg.request.Candidate.Id),
		}

		return (msg.request.LastLogIndex > r.LastLogIndex())
	}

	msg.reply <- RequestVoteReply{
		Term:        r.GetCurrentTerm(),
		VoteGranted: false,
	}
	return false
}
