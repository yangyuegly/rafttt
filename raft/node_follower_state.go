package raft

// doFollower implements the logic for a Raft node in the follower state.
func (r *Node) doFollower() stateFunction {
	r.Out("Transitioning to FollowerState")
	r.State = FollowerState

	timeout := randomTimeout(r.config.HeartbeatTimeout)

	for {
		select {
		case shutdown := <-r.gracefulExit:
			if shutdown {
				return nil
			}
		case appEn := <-r.appendEntries:
			resetTimeout, fallback := r.handleAppendEntries(appEn)
			if resetTimeout {
				timeout = randomTimeout(r.config.HeartbeatTimeout)
			}
		case reqVote := <-r.requestVote:
			currTerm := r.GetCurrentTerm()
			if reqVote.request.Term < currTerm {
				reqVote.reply <- RequestVoteReply{
					Term:        currTerm,
					VoteGranted: false,
				}
				continue
			}

			if reqVote.request.Term > currTerm {
				r.setCurrentTerm(reqVote.request.Term)
				r.setVotedFor("")
				currTerm = reqVote.request.Term
			}

			if r.isUpToDate(reqVote.request.LastLogTerm, reqVote.request.LastLogIndex) {
				voted := r.GetVotedFor()
				if voted == "" || voted == reqVote.request.Candidate.Id {
					r.setVotedFor(reqVote.request.Candidate.Id)
					reqVote.reply <- RequestVoteReply{
						Term:        currTerm,
						VoteGranted: true,
					}
					continue
				}
			}

			reqVote.reply <- RequestVoteReply{
				Term:        currTerm,
				VoteGranted: false,
			}
		case regCli := <-r.registerClient:
			regCli.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				ClientId:   0,
				LeaderHint: r.Leader,
			}
		case cliReq := <-r.clientRequest:
			cliReq.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   nil,
				LeaderHint: r.Leader,
			}
		case <-timeout:
			// timeout from leader occur, become a candidate
			return r.doCandidate
		}
	}
}

// return true if the log in the argument is at least up-to-date as our log
func (r *Node) isUpToDate(lastLogTerm uint64, lastLogIndex uint64) bool {
	myLastLogTerm := r.LastLogTerm()
	if lastLogTerm > myLastLogTerm {
		return true
	} else if lastLogTerm == myLastLogTerm && lastLogIndex >= r.LastLogIndex() {
		return true
	}
	return false
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	// TODO: Students should implement this method
	req := msg.request

	//if the request term is later, we should fallback
	if req.Term > r.GetCurrentTerm() {
		r.setCurrentTerm(req.Term)
		if r.State!= FollowerState {
			fallback = true
		}
	} 

	//if we are later than the request, append entries unsuccessful
	else if req.Term < r.GetCurrentTerm() {
		msg.reply<- AppendEntriesReply {
			Term: r.GetCurrentTerm()
			Success: false;
		}
		return false, fallback
	}
	else if req.PrevLogIndex>r.LastLogIndex() || req.PrevLogTerm!=r.LastLogTerm {
		msg.reply <- AppendEntriesReply{
			Term: r.GetCurrentTerm()
			Success: false;	
		}
		return false, fallback
	}



	start:= req.PrevLogIndex

	for l:= 0; && start< r.LastLogIndex(); l < len(req.GetEntries());l++ {
		start++; 
		if r.GetLog(req.GetEntries()[l].Index)!=req.GetEntries()[l]
	}

	TruncateLog()
	if req.LeaderCommit > r.commitIndex {
		commitIndex = math.Min(req.LeaderCommit, )
	}


	//successfully append log
	msg.reply <- AppendEntriesReply{
		Term: r.GetCurrentTerm()
		Success: true;	
	}
}
