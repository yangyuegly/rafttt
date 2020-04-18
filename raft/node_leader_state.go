package raft

import "time"

// doLeader implements the logic for a Raft node in the leader state.
func (r *Node) doLeader() stateFunction {
	r.Out("Transitioning to LeaderState")
	r.State = LeaderState
	r.Leader = r.Self

	r.leaderMutex.Lock()
	r.commitIndex = 0
	r.lastApplied = 0
	initNext := r.LastLogIndex() + 1
	for _, p := range r.Peers {
		r.nextIndex[p.Id] = initNext
		r.matchIndex[p.Id] = 0
	}
	r.matchIndex[r.Self.Id] = r.LastLogIndex()

	r.Out("Commiting the NOOP entry")
	r.StoreLog(&LogEntry{
		Index:   r.LastLogIndex() + 1,
		TermId:  r.GetCurrentTerm(),
		Type:    CommandType_NOOP,
		Command: 0,
		Data:    nil,
		CacheId: "",
	})
	r.leaderMutex.Unlock()
	fallback, sentToMajority := r.sendHeartbeats()
	if fallback {
		return r.doFollower
	}

	if !sentToMajority {
		r.Out("Warning: cannot talk to the majority of nodes")
	}

	ticker := time.NewTicker(r.config.HeartbeatTimeout / 2)
	defer ticker.Stop()

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
			r.Out("Registering Client")
			en := &LogEntry{
				Index:   r.LastLogIndex() + 1,
				TermId:  r.GetCurrentTerm(),
				Type:    CommandType_CLIENT_REGISTRATION,
				Command: 0,
				Data:    nil,
				CacheId: "",
			}

			r.StoreLog(en)

			fallback, sentToMajority := r.sendHeartbeats()
			if !fallback && sentToMajority {
				regCli.reply <- RegisterClientReply{
					Status:     ClientStatus_OK,
					ClientId:   en.Index,
					LeaderHint: r.Leader,
				}
			} else {
				regCli.reply <- RegisterClientReply{
					Status:     ClientStatus_REQ_FAILED,
					ClientId:   en.Index,
					LeaderHint: r.Leader,
				}
			}
		case cliReq := <-r.clientRequest:
			r.Out("Received Client Request %v", cliReq.request)
			cacheID := createCacheID(cliReq.request.ClientId, cliReq.request.SequenceNum)

			r.requestsMutex.Lock()
			oldReply, isDuplicate := r.GetCachedReply(*cliReq.request)
			if isDuplicate {
				// come after state machine runs
				cliReq.reply <- *oldReply
			} else if r.requestsByCacheID[cacheID] == nil {
				// the first request
				r.requestsByCacheID[cacheID] = cliReq.reply
			} else {
				// not the first request, but before the log precessed
				ch := make(chan ClientReply)
				r.requestsByCacheID[cacheID] = ch
				reply1 := r.requestsByCacheID[cacheID]
				reply2 := cliReq.reply
				go func() {
					msg := <-ch
					reply1 <- msg
					reply2 <- msg
				}()
			}
			r.requestsMutex.Unlock()

			en := &LogEntry{
				Index:   r.LastLogIndex() + 1,
				TermId:  r.GetCurrentTerm(),
				Type:    CommandType_STATE_MACHINE_COMMAND,
				Command: cliReq.request.StateMachineCmd,
				Data:    cliReq.request.Data,
				CacheId: cacheID,
			}

			r.StoreLog(en)

			fallback, _ := r.sendHeartbeats()

			if fallback {
				cliReq.reply <- ClientReply{
					Status:     ClientStatus_NOT_LEADER,
					Response:   nil,
					LeaderHint: r.Leader,
				}

				return r.doFollower
			}
		case <-ticker.C:
			//r.Out("Sending heartbeat")
			fallback, _ := r.sendHeartbeats()
			if fallback {
				return r.doFollower
			}
		}
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (r *Node) sendHeartbeats() (fallback, sentToMajority bool) {
	r.leaderMutex.Lock()
	r.nextIndex[r.Self.Id] = r.LastLogIndex() + 1
	r.matchIndex[r.Self.Id] = r.LastLogIndex()
	r.leaderMutex.Unlock()

	peersLen := len(r.Peers)
	doneCh := make(chan bool, peersLen)
	fallbackCh := make(chan bool, 1)

	for _, item := range r.Peers {
		if item.Id != r.Self.Id {
			p := item
			go func() {
				success := false
				defer func() {
					doneCh <- success
				}()

				for {
					// sent out everything from nextIndex -> lastLogIndex
					r.leaderMutex.Lock()
					nxtInd := r.nextIndex[p.Id]
					if nxtInd <= 0 {
						panic("Assertion nxtInd > 0 failed")
					}

					lastInd := r.LastLogIndex()
					var ensToSend []*LogEntry
					for i := nxtInd; i <= lastInd; i++ {
						ensToSend = append(ensToSend, r.GetLog(i))
					}

					var prevLogTerm uint64
					if r.GetLog(nxtInd-1) != nil {
						prevLogTerm = r.GetLog(nxtInd - 1).TermId
					} else {
						prevLogTerm = 0
					}
					r.leaderMutex.Unlock()

					req := &AppendEntriesRequest{
						Term:         r.GetCurrentTerm(),
						Leader:       r.Self,
						PrevLogIndex: nxtInd - 1,
						PrevLogTerm:  prevLogTerm,
						Entries:      ensToSend,
						LeaderCommit: r.commitIndex,
					}
					reply, err := p.AppendEntriesRPC(r, req)

					if err != nil {
						r.Out("AppendEntriesRPC to %v failed with %v", p.Id, err)
						return
					}

					success = reply.Success
					if reply.Term > r.GetCurrentTerm() {
						r.Out("falling back due to %v, from request %v", reply, req)
						r.setCurrentTerm(reply.Term)
						r.setVotedFor("")
						fallbackCh <- true
						return
					}

					if reply.Success {
						r.leaderMutex.Lock()
						r.nextIndex[p.Id] = lastInd + 1
						r.matchIndex[p.Id] = lastInd
						r.checkForCommit()
						r.leaderMutex.Unlock()
						return
					} else if nxtInd <= 1 {
						// can't go back anymore!!
						r.Error("AppendEntriesRPC to %v failed consistency check at 0", p.Id)
						return
					} else {
						r.leaderMutex.Lock()
						r.nextIndex[p.Id] = nxtInd - 1
						r.leaderMutex.Unlock()
					}

				}
			}()
		}
	}

	majority := r.config.ClusterSize/2 + 1
	successCnt := 1
	for i := 1; i < peersLen; i++ {
		select {
		case success := <-doneCh:
			if success {
				successCnt += 1
				if successCnt >= majority {
					return false, true
				}
			}
		case fall := <-fallbackCh:
			if fall {
				return true, false
			}
		}
	}

	return false, false
}

// should be called with leader mutex locked
func (r *Node) checkForCommit() {
	majority := r.config.ClusterSize/2 + 1

	newCommit := r.commitIndex
	for i := r.commitIndex + 1; i <= r.LastLogIndex(); i++ {
		cnt := 0
		for _, ind := range r.matchIndex {
			if ind >= i {
				cnt += 1
			}

			if cnt >= majority {
				newCommit = i
				break
			}
		}

		if newCommit != i {
			break
		}
	}

	if newCommit > r.commitIndex && r.GetLog(newCommit).TermId == r.GetCurrentTerm() {
		for i := r.commitIndex + 1; i <= newCommit; i++ {
			r.processLogEntry(*r.GetLog(i))
		}
		r.commitIndex = newCommit
	}
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (r *Node) processLogEntry(entry LogEntry) ClientReply {
	r.Out("Processing log entry: %v", entry)

	status := ClientStatus_OK
	response := []byte{}
	var err error

	// Apply command on state machine
	if entry.Type == CommandType_STATE_MACHINE_COMMAND {
		response, err = r.stateMachine.ApplyCommand(entry.Command, entry.Data)
		if err != nil {
			status = ClientStatus_REQ_FAILED
			response = []byte(err.Error())
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		Response:   response,
		LeaderHint: r.Self,
	}

	// Add reply to cache
	if entry.CacheId != "" {
		r.CacheClientReply(entry.CacheId, reply)
	}

	// Send reply to client
	r.requestsMutex.Lock()
	replyChan, exists := r.requestsByCacheID[entry.CacheId]
	if exists {
		replyChan <- reply
		delete(r.requestsByCacheID, entry.CacheId)
	}
	r.requestsMutex.Unlock()

	return reply
}
