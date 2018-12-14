package raft

import (
	"../infra"
	"../business_logic"
	"log"
	"time"
	"sync"
	"math/rand"
	"../../common"
	"strconv"
)



//A node only votes if the candidate has a longer node that it has
/*
Messages that a node can send:
	- Append Entry (empty): 
		- PrimaryType: "raft", 
		- SecType: "appendEntry", 
		- Request: raftMessage with Data: AppendMessage
	- Append Entry (with actual entries): 
		- PrimaryType: "raft", 
		- SecType: "fullAppend", 
		- Request: raftMessage with Data: AppendMessage
	- Vote Request:
		- PrimaryType: "raft", 
		- SecType: "voteRequest", 
		- Request: raftMessage
	- Log Recovery:
		- PrimaryType: "raft", 
		- SecType: "logRecovery", 
		- Request: raftMessage with Data: whole msgLog 

Messages that a node can recieve as responses:
	- Vote
		- PrimaryType: "response" (the arrival of the response is the message)
	- Append Ok
		- PrimaryType: "response"
		- SecType: "appendOk"
	- Inconsistent Log
		- PrimaryType: "response"
		- SecType: "inconsistentLog"

Possible Responses to Frontends:
	- Ok
		- PrimaryType: "ok"
		- Request: infra.ClientResponse
	- Not Leader
		- PrimaryType: leaderId
		- Request: new leader id (int)
	- Failure
		- leader can't reach a qourom
	- frontend may time out and try a different node, see frontend doc
*/


const LOGSIZE = 1000
const RAFTQUEUESIZE = 1000
const RESPONSETIMEOUT = 600

type Raft struct {
	//lock should only be locked in this order: Term, State, Log
	State int //0: follower, 1: candidate, 2: leader
	StateLock sync.Mutex
	Term int
	TermLock sync.Mutex
	MsgLog []*common.AppendMessage // each message here is located at LogNum-1
	LogNum int 
	LastCommittedLogNum int
	LogNumLock sync.Mutex
	LogConsistencyFlag bool // also controlled by the same mutex

	MsgQueue chan common.Message
	PingChan chan *common.Message
	LeaderId int
	LeaderIdLock sync.Mutex
	ElectionsKiller chan int

	LeaderPingerKiller chan int

}


//initialize the raft object with its channels and data
//lastCommitedLogNum and Log Num start at -1
//LogNum is always the last index appended (add 1 before sending)
func InitRaft() Raft {
	raft := Raft{State: 0, Term: 0, MsgQueue: make(chan common.Message, RAFTQUEUESIZE), PingChan: make(chan *common.Message, 10)}
	raft.Term = 0
	raft.LogNum = -1
	raft.LastCommittedLogNum = -1
	raft.ElectionsKiller = make(chan int, 1)
	raft.LeaderPingerKiller = make(chan int, 1)
	go raft.msgHandler()
	go raft.leaderTimer()
	return raft
}

//get the state of this node
func (raft *Raft) getState() int {
	raft.StateLock.Lock()
	defer raft.StateLock.Unlock()
	s := raft.State
	return s 
}

//set the state of this node
func (raft *Raft) setState(newState int) {
	raft.StateLock.Lock()
	defer raft.StateLock.Unlock()
	raft.State = newState
}

//get the node's term
func (raft *Raft) getTerm() int {
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	t := raft.Term
	return t
}

//set the term of this node
func (raft *Raft) setTerm(newTerm int) {
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	raft.Term = newTerm
}

//increment the node's term
func (raft *Raft) incrementTerm() int {
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	raft.Term += 1
	t := raft.Term
	return t
}

//get the current logNum (last index of inserted log - committed or uncommitted)
func (raft *Raft) getLogNum() int {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	n := raft.LogNum
	return n
}

//get the log index of the last committed entry
func (raft *Raft) getLastCommittedLogNum() int {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	n := raft.LastCommittedLogNum
	return n
}

//get a slice of all the entries that need to be committed
//some entries will be invalid by the reciever knows how to check for those
func (raft *Raft) getUncommitedLog() []*common.AppendMessage {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	if raft.LogNum == raft.LastCommittedLogNum {
		return []*common.AppendMessage{}
	} else {
		return raft.MsgLog[raft.LastCommittedLogNum + 1: raft.LogNum + 1]
	}
}


//get the leader's id
func (raft *Raft) getLeader() int {
	raft.LeaderIdLock.Lock()
	leader := raft.LeaderId
	raft.LeaderIdLock.Unlock()
	return leader
}

//change the leader's id (called when discovers the leader had changesd)
func (raft *Raft) setLeader(newLeader int) {
	log.Printf("My new oh mighty leader is node %d\n", newLeader)
	raft.LeaderIdLock.Lock()
	raft.LeaderId = newLeader
	raft.LeaderIdLock.Unlock()
}

//timeout on the leader, alive as long as entries keep coming on time
//stops and goes to elections when the timer ends
func (raft *Raft) leaderTimer () {
	log.Printf("RAFT: started leader timer")
	timer := getTimer(1)
	var message *common.Message
	for {
		select {
		case message = <-raft.PingChan:
			if message.NodeId == raft.getLeader() {
				timer = getTimer(1) //renew the timer for the next ping
			} 
			//else just ignore this ping
		case <-timer.C:
				log.Println("Leader timer ended")
				//go to elections
				go raft.toElection()
				return
		}
	}
}

//elections manager
//called once per election
//handles sending voteRequests, recieving, counting, and becoming a leader or a follower
func (raft *Raft) toElection() {

	raft.setState(1) //become a candidate
	thisTerm := raft.incrementTerm() //increment term
	voteRequest := common.RaftMessage{Term: thisTerm ,LastComittedLog: raft.getLastCommittedLogNum()}
	
	me := infra.MakeThread()
	defer infra.RemoveThread(me.Tid)
	votes := 1 // vote for myself
	neededVotes := infra.GetClusterSize() / 2 + 1

	//send voteRequests
	infra.QueueMessageForAll(&common.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "voteRequest", Request: voteRequest})
	timer := getTimer(1) //set elections timer
	var message *common.Message
	var vote common.RaftMessage
	log.Printf("RAFT: starting new elections with term: %d", thisTerm)
	for {
		select {
			case message = <-me.Responses:
				if message.PrimaryType == "response" { //could also be a failure but we don't care
					vote = message.Request.(common.RaftMessage)
					if vote.Term == thisTerm { // check that the vote is for this term
						votes += 1
						if votes >= neededVotes { //got enough votes to become a leader
							log.Printf("RAFT: I emerge victorious and becoming the leader of term %d", vote.Term)
							//if got here, election was won
							//become leader 
							raft.becomeLeader()
							return
						}
					}
				}
			case <- raft.ElectionsKiller: 
				//whoever did this is in charge of changing the state
				return
			case <-timer.C:
				log.Printf("Election timeout for term %d, going to elections...\n", thisTerm)
				raft.becomeFollower()

				return
		}
	}
}

//handles the voteRequest of a candidate node
func (raft *Raft) vote(message common.Message) {
	raftMessage, ok := message.Request.(common.RaftMessage)
	if !ok {
		log.Println("can't get raft message out of message")
		return
	}
	term := raft.getTerm()
	log.Printf("RAFT: got a vote request for term %d (Node %d) : %#v\n My term is %d\n", raftMessage.Term, message.NodeId ,message, term)
	

	if raftMessage.Term > term { //only vote for higher term nodes with last comitted log >= mine
		myLastCommittedLog := raft.getLastCommittedLogNum()
		if raftMessage.LastComittedLog < myLastCommittedLog {
			log.Printf("RAFT: Vote request has lower committed log than mine (vote with logNum %d, mine is %d), dropping\n", raftMessage.LastComittedLog, myLastCommittedLog)
			} else {
				//got an VoteRequest entry with larger term than mine
				//vote
				raft.setTerm(raftMessage.Term)
				response := &common.Message{Tid: message.Tid, PrimaryType: "response", Request: common.RaftMessage{Term: raft.Term}}
				conn := infra.GetConn(message.NodeId)
				infra.QueueMessage(conn.Queue, response)
				log.Printf("RAFT: Voted for term %d\n", raftMessage.Term)
			}
	} else {
		log.Printf("RAFT: Vote request has lower term than mine (vote for %d, mine is %d), dropping\n", raftMessage.Term, term)
	}
}


//handles periodic sending of append messages
//should only get started once node becomes leader
//sends a list of uncommited logs, and simlutaneously empty appends (pings) to make sure the followers are aware of this leader's existence
func (raft *Raft) sendAppendEntries() {
	term := raft.getTerm()
	me := infra.MakeThread()
	var votes int
	neededVotes := infra.GetClusterSize() / 2 + 1
	log.Printf("starting to send append entries with term %d\n", term)
	var votesTimer *time.Timer
	var votedTids []int32
	var votedNodes []int
	
	//this goroutine sends empty appends (pings)
	killEmptyAppends := make(chan int, 1)
	go func() {
		for {
			select {
				case <- killEmptyAppends:
					return
				default:
					var emptyLog []*common.AppendMessage
					raftMsg := common.RaftMessage{Term: term, Data: emptyLog , LastComittedLog: raft.getLastCommittedLogNum()}
					infra.QueueMessageForAll(&common.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "appendEntry", Request: raftMsg})
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	//repeatedly check if there are any uncommitted logs and distribute them
	for {
		start := time.Now()
		select {
			case <-raft.LeaderPingerKiller:
				log.Printf("Killed: stop sending appends for term %d. Timer elapsed: %#v\n", term, time.Since(start))
				killEmptyAppends <- 1
				return
			default:
				logToSend := raft.getUncommitedLog()
				if len(logToSend) > 0 { //send uncommitted entries
					raftMsg := common.RaftMessage{Term: term, Data: logToSend, LastComittedLog: raft.getLastCommittedLogNum()}
					infra.QueueMessageForAll(&common.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "fullAppend", Request: raftMsg})
					votes = 1 // vote for myself
					votedNodes = votedNodes[:0]
					votedTids = votedTids[:0]
					log.Printf("Waiting for qourom of Oks...")
					votesTimer = getTimer(3)
					SecondLoop:
					for {
						//wait for oks before sending commit (in which the logNums , start and end are specified)
						select {
						case response := <-me.Responses:
							log.Printf("Got a response from Node %d\n", response.NodeId)
								if response.SecType == "inconsistentLog" {
									go raft.sendLogForRecovery(response.NodeId)
								} else if response.SecType[:8] == "appendOk" {
									log.Printf("Node %d said ok\n", response.NodeId)
									targetTid, _ := strconv.Atoi(response.SecType[8:])
									votedTids = append(votedTids, int32(targetTid))
									votedNodes = append(votedNodes, response.NodeId)

									votes += 1
								}
						//time to count the result
						case <-votesTimer.C:
							if votes >= neededVotes {
								log.Printf("Got Qourom") //here send a commit message and don't worry about the response
								for i, _ := range(votedTids) {
									infra.QueueMessage(infra.GetConn(votedNodes[i]).Queue, &common.Message{
									Tid: votedTids[i],
									PrimaryType: "response",
									SecType: "commit",
									Request: common.CommitMessage{Start: raft.getLastCommittedLogNum()+ 1, End: raft.getLogNum()}}) //half open range
								}
								raft.commitMessages()
								break SecondLoop
							} else {
								log.Println("Giving up on appendOks....retrying")
							}
						case <-raft.LeaderPingerKiller:
							log.Printf("Killed: stop sending appends for term Timer elapsed: %#v\n", term, time.Since(start))
							//whoever did this will change the states
							killEmptyAppends <- 1
							return
						}
					}
				}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//get a random timer between 150-350 ms
//can take a divisor by which this interval can be divided (to get smaller random intervals)
func getTimer(divisor int) *time.Timer {
	timer := time.NewTimer(time.Duration( (rand.Intn(200) + 150) / divisor ) * time.Millisecond)
	return timer
}

//waits on the channel of a log entry until its committed or until the timeout
//used so the clientHandler can send a failure to the client and make that entry invalid in the future
// we don't want entries that resulted in a failure to later get committed
func (raft *Raft) waitForAppendToGetCommitted(logNum int) bool {
	timer := time.NewTimer(time.Duration( RESPONSETIMEOUT * time.Millisecond ) )
	Loop:
	for {
		select {
		case <-raft.MsgLog[logNum].GotCommitted:
			return true
		case <-timer.C:
			//mark msg ignored and return false
			
			break Loop
		}
	}
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	if raft.MsgLog[logNum].IsCommited {
		raft.MsgLog[logNum].ShouldIgnore = false //invalidate the entry
		return true
	} else {
		raft.MsgLog[logNum].ShouldIgnore = true
		return false
	}
}

//called by the leader's clientMessageHandler when it's notified that the client's messages is accepted by a qourum
func (raft *Raft) commitMessages() {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	i := raft.LastCommittedLogNum + 1
	log.Printf("Committing logs %d through %d inclusive\n", i, raft.LogNum)
	for i <= raft.LogNum {
		raft.MsgLog[i].IsCommited = true
		raft.MsgLog[i].GotCommitted <- 1
		raft.LastCommittedLogNum = i
		business_logic.ProcessWrite(&raft.MsgLog[i].Msg) //process by the business logic (actual db)
		i++
	}
	raft.LastCommittedLogNum = raft.LogNum
}

//called by followers when the get a commit message on a previously approved entry slice
//adds the entries to the log and process them
//ignores invalid entries (which were added, uncommitted, but timed out on the client)
func (raft *Raft) extendLogAndCommit(appendLog []*common.AppendMessage) { //not thread safe (need to acquire lock before calling)
	log.Printf("Committing NEW logs %d through %d inclusive\n", appendLog[0].LogNum, appendLog[len(appendLog) - 1].LogNum)
	raft.MsgLog = raft.MsgLog[:raft.LastCommittedLogNum + 1] // ensures that all uncommitted logs are deleted
	for _, apnd := range(appendLog) {
		apnd.IsCommited = true
		raft.MsgLog = append(raft.MsgLog, apnd)
		raft.LastCommittedLogNum = apnd.LogNum
	}
	raft.LogNum = raft.LastCommittedLogNum

	//process the appends into the db
	for _, apnd := range(appendLog) {
		if !apnd.ShouldIgnore {
			business_logic.ProcessWrite(&apnd.Msg)
		}
	}
}

//handles an incoming append entry message
//decides whether to accept it or ask for the log for recovery
func (raft *Raft) appendEntryHandler(message common.Message) {
	raftMsg, _ := message.Request.(common.RaftMessage)
	appndLog, _ := raftMsg.Data.([]*common.AppendMessage)
	raft.PingChan <- &message // notify the leaderTimer that a message is in immediately
	//log.Printf("notified pinger thread")
	term := raft.getTerm()
	if len(appndLog) > 0 {log.Println("Got non empty append0") }
	//state := raft.getState()
	logNum := raft.getLogNum()
	state := raft.getState()
	me := infra.MakeThread()
	defer infra.RemoveThread(me.Tid)
	myLastCommitedLogNum := raft.getLastCommittedLogNum()
	//log.Printf("Processing append...%#v\n", *message)
	if len(appndLog) > 0 {log.Println("Got non empty append1") }
	
	//ensure the message is valid
	//if the sender is of higher term and valid log, accept it as the new leader
	if (raftMsg.Term >= term && raftMsg.LastComittedLog == myLastCommitedLogNum)  || raftMsg.LastComittedLog > myLastCommitedLogNum {
		if state != 0 {
			if state == 1 { //if candidate, kill elections
				raft.ElectionsKiller <- 1
			} else if state == 2 { // if leader, relinquish
				raft.LeaderPingerKiller <- 1		
			}
			raft.setTerm(raftMsg.Term)
			raft.setLeader(message.NodeId)
			raft.becomeFollower() //will only be able to start after StateLock is released
		}
	}
	

	if raftMsg.Term >= term {
		if raftMsg.Term > term && raft.getLeader() != message.NodeId {
			raft.setLeader(message.NodeId) //accept this as a new leader
		}
		if len(appndLog) > 0 {log.Println("Got non empty append2") } //for debugging actual incoming client messages
		if len(appndLog) == 0 {
			// this means the append entry is empty, just a ping
			if raftMsg.LastComittedLog != myLastCommitedLogNum {
				log.Printf("Got a new appendEntry (log %d) inconsistent with my log (log %d)", raftMsg.LastComittedLog, raft.getLastCommittedLogNum())
				//respond with recovery request
				raft.requestLogRecovery(message.NodeId)
			}

		} else if appndLog[0].LogNum == logNum + 1 {
			//if log entry is one larger than mine, append it and process
			log.Printf("Got a new appendEntry to the log: %#v\n", appndLog)

			//respond
			leaderConn := infra.GetConn(message.NodeId)
			log.Printf("Responding with thread Id: %s\n", strconv.Itoa(int(me.Tid)))
			infra.QueueMessage(leaderConn.Queue, &common.Message{
					Tid: message.Tid,
					PrimaryType: "response",
					SecType: "appendOk" + strconv.Itoa(int(me.Tid)),
					Request: term})
			log.Printf("Sent Ok to leader %d\n", message.NodeId)
			//now wait for commit
			<- me.Responses
			log.Printf("Got a commit message")
			raft.extendLogAndCommit(appndLog)

		} else if appndLog[0].LogNum > logNum || appndLog[0].LogNum < logNum{
			//respond with recovery request
			log.Printf("Got a new appendEntry (log %d) inconsistent with my log (log %d). Asking for recovery", appndLog[0].LogNum, raft.LogNum)
			raft.respondWithLogRecovery(message.NodeId, message.Tid)
		} 
		
	}
	//if got ping of a lower term, drop it
}

//send a follower the whole log for recovery (a client my ask spotaneously or as a reponse to an append))
func (raft *Raft) sendLogForRecovery(nodeId int) {
	log.Printf("Sending log recovery for node id: %d", nodeId)
	term := raft.getTerm()
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	conn := infra.GetConn(nodeId)
	msg := common.Message{
			PrimaryType: "raft", 
			SecType: "logRecovery", 
			Request: common.RaftMessage{
				Term: term,
				Data: raft.MsgLog[:raft.LastCommittedLogNum + 1]}}
	infra.QueueMessage(conn.Queue, &msg)
}

//apply a log recovery message. Clear the log, add the entries, and process the valid ones
func (raft *Raft) logRecovery(raftMsg common.RaftMessage) {
	log.Println("RAFT: Got log for recovery...recovering..")
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()

	msgLog, _ := raftMsg.Data.([]*common.AppendMessage)
	raft.MsgLog = msgLog
	raft.LogNum = raft.MsgLog[len(raft.MsgLog)-1].LogNum
	raft.LastCommittedLogNum = raft.LogNum
	business_logic.Reprocess(msgLog)
	log.Println("RAFT: Finished recovering my log..")
	return
}

//ask for a log recovery from the leader
func (raft *Raft) requestLogRecovery(nodeId int) {
	log.Printf("Requesting log recovery")
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	leaderConn := infra.GetConn(nodeId)
	infra.QueueMessage(leaderConn.Queue, &common.Message{
			PrimaryType: "raft",
			SecType: "inconsistentLog",
			Request: common.RaftMessage{}})
	log.Printf("Requested log recovery")
}

//respond to a client requesting log recovery (a client my ask spotaneously or as a reponse to an append)
func (raft *Raft) respondWithLogRecovery(nodeId int, targetThreadId int32) {
	log.Printf("Sending log recovery")
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	leaderConn := infra.GetConn(nodeId)
	infra.QueueMessage(leaderConn.Queue, &common.Message{
			Tid: targetThreadId,
			PrimaryType: "response",
			SecType: "inconsistentLog",
			Request: common.RaftMessage{}})
	log.Printf("Responded with log recovery")
}

//become a follower
func (raft *Raft) becomeFollower() {
	log.Println("Becoming follower")
	raft.setState(0)
	go raft.leaderTimer()
}

//become a leader
func (raft *Raft) becomeLeader() {
	raft.setState(2)
	go raft.sendAppendEntries()
}

//takes messages from the channel given to the infra package and dispatches appropriate nodes
func (raft *Raft) msgHandler() { //consider running many of these
	var message common.Message
	var raftMessage common.RaftMessage
	for {
		message = <-raft.MsgQueue
		switch message.PrimaryType {
		case "raft":
			raftMessage = message.Request.(common.RaftMessage)
			if message.SecType == "fullAppend" {
				log.Printf("about to swithc to Full APPEND!\n")
			}
			//log.Printf("SecType: %s\n", message.SecType)
			switch message.SecType {
			case "logRecovery":
				go raft.logRecovery(raftMessage)
			case "fullAppend":
				log.Printf("FULLLLL APPEND!\n")
				go raft.appendEntryHandler(message)
			case "appendEntry":
				go raft.appendEntryHandler(message)
			case "voteRequest":
				go raft.vote(message)
			case "inconsistentLog":
				go raft.sendLogForRecovery(message.NodeId)
			} 
		case "client":
			go raft.clientMessageHandler(message) 
		}
	}
}

//when clients sends message, log is appended uncommited 
//the sendAppendEntries routine will take care of getting it committed
//if a message can't get committed in a certain time, it becomes invalid for all future committing
func (raft *Raft) clientMessageHandler(message common.Message) {
	//REMEMBER: if at any point node is not a leader anymore, stop and reply with the leader's address 
	clientMessage := message.Request.(common.ClientMessage)
	log.Println(clientMessage)

	//first spin if I am a candidate
	term := raft.getTerm()
	raft.StateLock.Lock()
	if raft.State == 1 {
		log.Println("I'm a candidate...waiting for a resolution before responding to client")
		raft.StateLock.Unlock()
		for {
			raft.StateLock.Lock()
			if raft.State != 1 {
				break
			}
			raft.StateLock.Unlock()
		}
	} 

	//at this point the State is locked 

	if raft.State == 0 {
		log.Printf("I'm a follower..sending leader id %d to client\n", raft.getLeader())
		//if I am not the leader, respond with the leader id
		Newmessage := common.Message{PrimaryType: "leaderId", Request: raft.getLeader()}
		clientMessage.Response <- &Newmessage
		raft.StateLock.Unlock()

	} else { //I am the leader

		//if read, process and respond
		if business_logic.IsRead(&clientMessage) {
			raft.StateLock.Unlock()
			log.Printf("I'm a leader..responding to read request...\n")
			res := business_logic.ProcessRead(&clientMessage)
			message := common.Message{PrimaryType: "ok", Request: res}
			clientMessage.Response <- &message
		} else {
			log.Printf("I'm a leader..waiting for a quorom for write request...\n")
			raft.LogNumLock.Lock()

			//append the message to the log and let the pinger distribute it
			raft.LogNum += 1
			logNum := raft.LogNum
			raft.MsgLog = append(raft.MsgLog, &common.AppendMessage{Term: term, LogNum: raft.LogNum, Msg: clientMessage, ShouldIgnore: false, GotCommitted: make(chan int, 1)})		
			log.Printf("Appended log %d\n", raft.LogNum)
			raft.StateLock.Unlock()
			raft.LogNumLock.Unlock()
			log.Printf("Waiting for log: %d to get committed\n", logNum)

			//wait for the message to get committed or time out
			ok := raft.waitForAppendToGetCommitted(logNum)
			if ok {
				log.Printf("Got a quorom...responding to client\n")
				Newmessage := common.Message{PrimaryType: "ok"}
				clientMessage.Response <- &Newmessage
			} else {
				log.Printf("Couldn't get a quorom in time...responding to client\n")
				Newmessage := common.Message{PrimaryType: "failure"}
				clientMessage.Response <- &Newmessage
			}			
		}
		
	} 
}

