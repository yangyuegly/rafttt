package raft

// doFollower implements the logic for a Raft node in the follower state.
func (r *Node) doFollower() stateFunction {
	r.Out("Transitioning to FollowerState")
	r.State = FollowerState

	// flush all the cached request
	for k, v := range r.requestsByCacheID {
		v <- ClientReply{
			Status:     ClientStatus_NOT_LEADER,
			Response:   nil,
			LeaderHint: r.Leader,
		}
		delete(r.requestsByCacheID, k)
	}

	timeout := randomTimeout(r.config.ElectionTimeout)

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
				timeout = randomTimeout(r.config.ElectionTimeout)
			}
		case reqVote := <-r.requestVote:
			voteGranted, currTerm := r.processVoteRequest(reqVote.request)

			if voteGranted {
				r.Out("Resetting Timeout")
				timeout = randomTimeout(r.config.ElectionTimeout)
			}

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
func (r *Node) processVoteRequest(req *RequestVoteRequest) (voteGranted bool, term uint64) {
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
		r.setVotedFor("")
		currTerm = req.Term
	}

	if r.isUpToDate(req.LastLogTerm, req.LastLogIndex) {
		if voted == "" || voted == req.Candidate.Id {
			r.Out("request term up-to-date and has not voted: grant vote")
			r.setVotedFor(req.Candidate.Id)
			return true, currTerm
		}
	}
	return false, r.GetCurrentTerm()
}

// return true if the log in the argument is at least up-to-date as our log
func (r *Node) isUpToDate(lastLogTerm uint64, lastLogIndex uint64) bool {
	if lastLogTerm > r.LastLogTerm() {
		return true
	} else if lastLogTerm == r.LastLogTerm() {
		if lastLogIndex >= r.LastLogIndex() {
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
	resetTimeout = false

	if req.Term < r.GetCurrentTerm() {
		// if we are later than the request, append entries unsuccessful
		msg.reply <- AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return false, false
	}
	// if the request term is later, we should fallback
	if req.Term > r.GetCurrentTerm() {
		r.setCurrentTerm(req.Term)
		r.setVotedFor("")
		if r.State != FollowerState {
			fallback = true
		}
		resetTimeout = true
	} else {
		// req.Term == r.GetCurrentTerm()
		if r.State == CandidateState {
			fallback = true
		} else if r.State == LeaderState {
			panic("Two leaders found for the same term!")
		}
	}

	r.Leader = req.Leader
	// receiver implementation 2
	prevLog := r.GetLog(req.PrevLogIndex)
	if prevLog == nil || prevLog.TermId != req.PrevLogTerm {
		// r.TruncateLog(req.PrevLogIndex)
		msg.reply <- AppendEntriesReply{
			Term:    r.GetCurrentTerm(),
			Success: false,
		}
		return true, fallback
	}
	// implmentation 3-5
	// check for conflict
	for _, en := range req.Entries {
		if r.GetLog(en.Index) != nil && r.GetLog(en.Index).TermId != en.TermId {
			r.TruncateLog(en.Index)
			break
		}
	}

	// append entries
	lastNewEntry := r.LastLogIndex()
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

		for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
			if r.GetLog(i) != nil {
				r.processLogEntry(*r.GetLog(i))
			}
			r.lastApplied = i
		}
	}

	//successfully append log
	msg.reply <- AppendEntriesReply{
		Term:    r.GetCurrentTerm(),
		Success: true,
	}
	return true, true
}
