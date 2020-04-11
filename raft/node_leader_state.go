package raft

import "time"

// doLeader implements the logic for a Raft node in the leader state.
func (r *Node) doLeader() stateFunction {
	r.Out("Transitioning to LeaderState")
	r.State = LeaderState
	r.Leader = r.Self
	fallback, _ := r.sendHeartbeats()
	if fallback {
		return r.doFollower
	}

	r.commitIndex = 0
	r.lastApplied = 0
	initNext := r.LastLogIndex() + 1
	for _, p := range r.Peers {
		r.nextIndex[p.Id] = initNext
		r.matchIndex[p.Id] = 0
	}
	r.matchIndex[r.Self.Id] = r.LastLogIndex()

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
			// TODO: MUTEXS!!
			// requestsMutex     sync.Mutex

			cacheID := createCacheID(cliReq.request.ClientId, cliReq.request.SequenceNum)
			// TODO: make sure to not duplicate request!
			en := &LogEntry{
				Index:   r.LastLogIndex() + 1,
				TermId:  r.GetCurrentTerm(),
				Type:    CommandType_STATE_MACHINE_COMMAND,
				Command: cliReq.request.StateMachineCmd,
				Data:    cliReq.request.Data,
				CacheId: cacheID,
			}

			r.StoreLog(en)
			r.requestsByCacheID[cacheID] = cliReq.reply

			r.sendHeartbeats()

			r.processLogEntry(entry)
		case <-ticker.C:
			fallback, _ := r.sendHeartbeats()
			if fallback {
				return r.doFollower
			}
		}
	}

	return nil
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
	r.nextIndex[r.Self.Id] = r.LastLogIndex() + 1
	r.matchIndex[r.Self.Id] = r.LastLogIndex()

	for _, p := range r.Peers {
		if p != r.Self {
			go func() {
				// TODO: check for terminating condition

				for {
					// sent out everything from nextIndex -> lastLogIndex
					nxtInd := r.nextIndex[p.Id]
					lastInd := r.LastLogIndex()
					var ensToSend []*LogEntry
					for i := nxtInd; i <= lastInd; i++ {
						// TODO: what if getlog nil?
						ensToSend = append(ensToSend, r.GetLog(i))
					}

					reply, err := p.AppendEntriesRPC(r, &AppendEntriesRequest{
						Term:         r.GetCurrentTerm(),
						Leader:       r.Self,
						PrevLogIndex: nxtInd - 1,
						// TODO: what if getlog nil?
						PrevLogTerm:  r.GetLog(nxtInd - 1).TermId,
						Entries:      ensToSend,
						LeaderCommit: r.commitIndex,
					})

					if err != nil {
						r.Out("AppendEntriesRPC to %v failed with %v", p.Id, err)
						return
					}

					if reply.Term > r.GetCurrentTerm() {
						r.setCurrentTerm(reply.Term)
						r.setVotedFor("")
						// fallback!!
						// TODO: fix this
						fallback = true
					}

					if reply.Success {
						r.nextIndex[p.Id] = lastInd + 1
						r.matchIndex[p.Id] = lastInd
						break
					} else {
						r.nextIndex[p.Id] = nxtInd - 1
					}

				}
			}()
		}
	}

	var N uint64
	quorum := r.config.ClusterSize/2 + 1
	for i := r.commitIndex + 1; i < len(r.Entries); i++ {
		count := 0
		for _, item := range r.matchIndex {
			if count >= quorum {
				N = i
				break
			}
			if item >= i {
				N++
			}
		}
	}

	return true, true
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
