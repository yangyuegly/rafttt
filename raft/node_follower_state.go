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
			resetTimeout, _ := r.handleAppendEntries(appEn)
			if resetTimeout {
				//r.Out("Resetting Timeout")
				timeout = randomTimeout(r.config.HeartbeatTimeout)
			}
		case reqVote := <-r.requestVote:
			voteGranted, currTerm := r.processVoteRequest(reqVote.request)
			reqVote.reply <- RequestVoteReply{
				Term:        currTerm,
				VoteGranted: voteGranted,
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
			r.Out("Heartbeat Timeout, becoming candidate")
			return r.doCandidate
		}
	}
}
func (r *Node) processVoteRequest(req *RequestVoteRequest) (bool, uint64) {
	voted := r.GetVotedFor()
	r.Out("Receiving Request Vote: %v", req)
	currTerm := r.GetCurrentTerm()
	if req.Term < r.GetCurrentTerm() {
		r.Out("request term lower: reject vote")
		return false, currTerm
	}

	if req.Term > currTerm {
		r.Out("request term higher and has not voted: update our term")
		r.setCurrentTerm(req.Term)
		currTerm = req.Term
	}

	if r.isUpToDate(req.LastLogTerm, req.LastLogIndex) {
		r.Out("request term up-to-date and has not voted: grant vote")
		if voted == "" || voted == req.Candidate.Id {
			r.setVotedFor(req.Candidate.Id)
			return true, currTerm
		}
	}
	return false, r.GetCurrentTerm()
}

// return true if the log in the argument is at least up-to-date as our log
func (r *Node) isUpToDate(lastLogTerm uint64, lastLogIndex uint64) bool {
	myLastLogTerm := r.LastLogTerm()

	if lastLogTerm > myLastLogTerm {
		r.Out("their last log term is greater")
		return true
	} else if lastLogTerm == myLastLogTerm {
		if lastLogIndex >= r.LastLogIndex() {
			r.Out("last log term is equal, their last log index is at least ours")
			return true
		}
	}
	return false
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (r *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	//r.Out("Receiving Append Entries: %v", msg.request)
	req := msg.request
	fallback = false

	// if the request term is later, we should fallback
	if req.Term > r.GetCurrentTerm() {
		r.setCurrentTerm(req.Term)
		r.setVotedFor("")
		fallback = true
	} else if r.State == CandidateState && req.Term == r.GetCurrentTerm() {
		fallback = true
	} else if req.Term < r.GetCurrentTerm() {
		// if we are later than the request, append entries unsuccessful
		msg.reply <- AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return false, fallback
	}

	r.Leader = req.Leader
	prevLog := r.GetLog(req.PrevLogIndex)
	if prevLog == nil || prevLog.TermId != req.PrevLogTerm {
		r.TruncateLog(req.PrevLogIndex)
		msg.reply <- AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return true, fallback
	}
	// check for conflict
	for _, en := range req.Entries {
		if r.GetLog(en.Index) != nil && r.GetLog(en.Index).TermId != en.TermId {
			r.TruncateLog(en.Index)
			break
		}
	}

	// append entries
	var lastNewEntry uint64
	for _, en := range req.Entries {
		if r.GetLog(en.Index) == nil {
			r.StoreLog(en)
		}
		lastNewEntry = en.Index
	}

	if req.LeaderCommit > r.commitIndex {
		if req.LeaderCommit > lastNewEntry {
			r.commitIndex = lastNewEntry
		} else {
			r.commitIndex = req.LeaderCommit
		}
	}

	//successfully append log
	msg.reply <- AppendEntriesReply{
		Term:    r.GetCurrentTerm(),
		Success: true,
	}
	return true, fallback
}
