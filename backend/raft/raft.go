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
	- Append Entry: 
		- PrimaryType: "raft", 
		- SecType: "appendEntry", 
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




/*

appendlog function that takes a list of logs and appends it (for accepting logs)

when clients sends message:
log is appended uncommited easily (one at a time)


sendAppend:
sends a list of uncommited logs
keep the same logNum logic, always send logNum
wait for oks before sending commit (in which the logNums , start and end are specified)



*/

//Raft Logic 
const LOGSIZE = 1000
const RAFTQUEUESIZE = 1000

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

	MsgQueue chan *common.Message
	PingChan chan *common.RaftMessage
	LeaderId int
	LeaderIdLock sync.Mutex
	ElectionsKiller chan int

	LeaderPingerKiller chan int

}

func InitRaft() Raft {
	raft := Raft{State: 0, Term: 0, MsgQueue: make(chan *common.Message, RAFTQUEUESIZE), PingChan: make(chan *common.RaftMessage, 1)}
	raft.Term = 0
	raft.LogNum = -1
	raft.LastCommittedLogNum = -1
	raft.ElectionsKiller = make(chan int, 1)
	raft.LeaderPingerKiller = make(chan int, 1)
	go raft.msgHandler()
	go raft.leaderTimer()
	return raft
}

func (raft *Raft) getState() int {
	raft.StateLock.Lock()
	defer raft.StateLock.Unlock()
	s := raft.State
	return s 
}

func (raft *Raft) setState(newState int) {
	raft.StateLock.Lock()
	defer raft.StateLock.Unlock()
	raft.State = newState
}

func (raft *Raft) getTerm() int {
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	t := raft.Term
	return t
}

func (raft *Raft) getLogNum() int {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	n := raft.LogNum
	return n
}

func (raft *Raft) getLastCommittedLogNum() int {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	n := raft.LastCommittedLogNum
	return n
}

func (raft *Raft) getUncommitedLog() []*common.AppendMessage {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	if raft.LogNum == raft.LastCommittedLogNum {
		return []*common.AppendMessage{}
	} else {
		return raft.MsgLog[raft.LastCommittedLogNum + 1: raft.LogNum + 1]
	}
}

func (raft *Raft) setTerm(newTerm int) {
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	raft.Term = newTerm
}

func (raft *Raft) getLeader() int {
	raft.LeaderIdLock.Lock()
	leader := raft.LeaderId
	raft.LeaderIdLock.Unlock()
	return leader
}

func (raft *Raft) setLeader(newLeader int) {
	log.Printf("My new oh mighty leader is node %d\n", newLeader)
	raft.LeaderIdLock.Lock()
	raft.LeaderId = newLeader
	raft.LeaderIdLock.Unlock()
}

func (raft *Raft) leaderTimer () {
	log.Printf("RAFT: started leader timer")
	timer := getTimer(1)
	var message *common.RaftMessage
	//start := time.Now()
	for {
		
		select {
		case message = <-raft.PingChan:
			//check term number
			log.Printf("Got append with term %d, my term is %d\n", message.Term, raft.Term)
			if message.Term >= raft.Term {
				//got a valid Append entry 
				//accept this as my leader
				raft.setTerm(message.Term)
				//start = time.Now()
				timer = getTimer(1)
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

func (raft *Raft) toElection() {
	raft.TermLock.Lock()

	raft.setState(1)
	raft.Term += 1
	thisTerm := raft.Term
	voteRequest := common.RaftMessage{Term: raft.Term, LastComittedLog: raft.getLastCommittedLogNum()}
	raft.TermLock.Unlock()
	
	me := infra.MakeThread()
	defer infra.RemoveThread(me.Tid)
	votes := 1 // vote for myself
	neededVotes := infra.GetClusterSize() / 2 + 1

	infra.QueueMessageForAll(&common.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "voteRequest", Request: voteRequest})
	timer := getTimer(1)
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
						if votes >= neededVotes {
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

func (raft *Raft) vote(message *common.Message) {
	raftMessage, ok := message.Request.(common.RaftMessage)
	if !ok {
		log.Println("can't get raft message out of message")
		return
	}
	term := raft.getTerm()
	log.Printf("RAFT: got a vote request for term %d (Node %d) : %#v\n My term is %d\n", raftMessage.Term, message.NodeId ,*message, term)
	

	if raftMessage.Term > term && raftMessage.LastComittedLog >= raft.getLastCommittedLogNum() { //only vote for higher term nodes with last comitted log >= mine
		//got an VoteRequest entry with larger term than mine
		//voteYes
		raft.setTerm(raftMessage.Term)
		response := &common.Message{Tid: message.Tid, PrimaryType: "response", Request: common.RaftMessage{Term: raft.Term}}
		conn := infra.GetConn(message.NodeId)
		infra.QueueMessage(conn.Queue, response)
		log.Printf("RAFT: Voted for term %d\n", raftMessage.Term)


	} else {
		log.Printf("RAFT: Vote request has lower term than mine (vote for %d, mine is %d), dropping\n", raftMessage.Term, term)
	}
}

//should only get started once node becomes leader
func (raft *Raft) sendAppendEntries() {
	term := raft.getTerm()
	me := infra.MakeThread()
	var votes int
	neededVotes := infra.GetClusterSize() / 2 + 1
	log.Printf("starting to send append entries with term %d\n", term)
	var votesTimer *time.Timer
	var votedTids []int32
	var votedNodes []int
	for {
		start := time.Now()
		select {
			case <-raft.LeaderPingerKiller:
				log.Printf("Killed: stop sending appends for term %d. Timer elapsed: %#v\n", term, time.Since(start))
				return
			default:
				logToSend := raft.getUncommitedLog()
				raftMsg := common.RaftMessage{Term: term, Data: logToSend, LastComittedLog: raft.getLastCommittedLogNum()}
				infra.QueueMessageForAll(&common.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "appendEntry", Request: raftMsg})
				
				if len(logToSend) > 0 {
					votes = 1 // vote for myself
					votedNodes = votedNodes[:0]
					votedTids = votedTids[:0]
					log.Printf("Waiting for qourom of Oks...")
					votesTimer = getTimer(3)
					SecondLoop:
					for {
						select {
						case response := <-me.Responses:
							responseTerm, _ := response.Request.(int)
							if responseTerm == term {
								if response.SecType == "inconsistentLog" {
									go raft.sendLogForRecovery(response.NodeId)
								} else if response.SecType[:8] == "appendOk" {
									targetTid, _ := strconv.Atoi(response.SecType[8:])
									votedTids = append(votedTids, int32(targetTid))
									votedNodes = append(votedNodes, response.NodeId)

									votes += 1
								}
							}
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
							}
						case <-raft.LeaderPingerKiller:
							log.Printf("Killed: stop sending appends for term Timer elapsed: %#v\n", term, time.Since(start))
							//whoever did this will change the states
							return
						}
					}
				}
		}
		time.Sleep(50 * time.Millisecond)
	}
}


func getTimer(divisor int) *time.Timer {
	timer := time.NewTimer(time.Duration( (rand.Intn(200) + 150) / divisor ) * time.Millisecond)
	return timer
}

func (raft *Raft) waitForAppendToGetCommitted(logNum int) {
	raft.LogNumLock.Lock()
	for !raft.MsgLog[logNum].IsCommited {
		raft.LogNumLock.Unlock()
		raft.LogNumLock.Lock()
	}
	raft.LogNumLock.Unlock()
}


//lastCommitedLogNum and Log Num start at -1
//LogNum is always the last index appended (add 1 before sending)

func (raft *Raft) commitMessages() {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	i := raft.LastCommittedLogNum + 1
	log.Printf("Committing logs %d through %d inclusive\n", i, raft.LogNum)
	for i <= raft.LogNum {
		raft.MsgLog[i].IsCommited = true
		raft.LastCommittedLogNum = i
		i++
	}
	raft.LastCommittedLogNum = raft.LogNum

}

func (raft *Raft) extendLogAndCommit(appendLog []*common.AppendMessage) { //not thread safe (need to acquire lock before calling)
	for _, apnd := range(appendLog) {
		apnd.IsCommited = true
		raft.MsgLog = append(raft.MsgLog, apnd)
		raft.LastCommittedLogNum = apnd.LogNum
	}
	raft.LogNum = raft.LastCommittedLogNum

	//print the state of the log
	

	//process the appends
	for _, apnd := range(appendLog) {
		business_logic.ProcessMsg(&apnd.Msg)
	}

	//print the state of the db
}
func (raft *Raft) appendEntryHandler(message *common.Message) {
	//this function assumes that only the leader (or someone that should be the leader) 
	//sends append messages
	log.Printf("got append")
	//if got ping of higher term an am leader / in election - kill channels and become follower
	raftMsg, _ := message.Request.(common.RaftMessage)
	raft.PingChan <- &raftMsg // notify the leaderTimer that a message is in
	log.Printf("notified pinger thread")
	term := raft.getTerm()
	state := raft.getState()
	logNum := raft.getLogNum()
	me := infra.MakeThread()
	defer infra.RemoveThread(me.Tid)

	if state != 0 && raftMsg.Term >= term && raftMsg.LastComittedLog >= raft.getLastCommittedLogNum() { 
		log.Printf("Got a message with a higher term than me: %#v\n", *message)
		if state == 1 { //if candidate, kill elections
			raft.ElectionsKiller <- 1
		} else if state == 2 { // if leader, relinquish
			raft.LeaderPingerKiller <- 1		}

		raft.setTerm(raftMsg.Term)
		raft.setLeader(message.NodeId)
		raft.becomeFollower() //will only be able to start after StateLock is released
	} 
	

	log.Printf("Processing append...")
	if raftMsg.Term >= term {
		if raft.getLeader() != message.NodeId {
			raft.setLeader(message.NodeId)
		}
		appndLog, _ := raftMsg.Data.([]*common.AppendMessage)

		if len(appndLog) == 0 {
			// this means the append entry is empty, just a ping
			if raftMsg.LastComittedLog != raft.getLastCommittedLogNum() {
				log.Printf("Got a new appendEntry (log %d) inconsistent with my log (log %d)", raftMsg.LastComittedLog, raft.getLastCommittedLogNum())
				//respond with recovery request
				raft.requestLogRecovery(message.NodeId)
			}

		} else if appndLog[0].LogNum == logNum + 1 {
			//if log entry is one larger than mine, append it and process
			log.Printf("Got a new appendEntry to the log: %#v\n", appndLog)

			//respond
			leaderConn := infra.GetConn(message.NodeId)
			log.Printf("Respinding with: %s\n", strconv.Itoa(int(me.Tid)))
			infra.QueueMessage(leaderConn.Queue, &common.Message{
					Tid: message.Tid,
					PrimaryType: "response",
					SecType: "appendOk" + strconv.Itoa(int(me.Tid)),
					Request: term})

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
func (raft *Raft) logRecovery(raftMsg * common.RaftMessage) {
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

func (raft *Raft) requestLogRecovery(nodeId int) {
	raft.LogNumLock.Lock()
	defer raft.LogNumLock.Unlock()
	leaderConn := infra.GetConn(nodeId)
	infra.QueueMessage(leaderConn.Queue, &common.Message{
			PrimaryType: "raft",
			SecType: "inconsistentLog",
			Request: common.RaftMessage{}})
	log.Printf("Requested log recovery")
}
func (raft *Raft) respondWithLogRecovery(nodeId int, targetThreadId int32) {
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

func (raft *Raft) becomeFollower() {
	log.Println("Becoming follower")
	raft.setState(0)
	go raft.leaderTimer()
}

func (raft *Raft) becomeLeader() {
	raft.setState(2)
	go raft.sendAppendEntries()
}

func (raft *Raft) msgHandler() { //consider running many of there
	var message *common.Message
	var raftMessage common.RaftMessage
	for {
		select {
			case message = <-raft.MsgQueue:
				switch message.PrimaryType {
				case "raft":
					raftMessage = message.Request.(common.RaftMessage)
					switch message.SecType {
					case "logRecovery":
						go raft.logRecovery(&raftMessage)
					case "appendEntry":
						go raft.appendEntryHandler(message)
					case "voteRequest":
						go raft.vote(message)
					case "inconsistentLog":
						go raft.sendLogForRecovery(message.NodeId)
					} 
				case "client":
					go raft.clientMessageHandler(message) //TODO: get rid of this, it should never get here
				}
		}
	}
}

func (raft *Raft) clientMessageHandler(message *common.Message) {
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
		message := common.Message{PrimaryType: "leaderId", Request: raft.getLeader()}
		clientMessage.Response <- &message
		raft.StateLock.Unlock()

	} else { //I am the leader



		//if read, process and respond
		if business_logic.IsRead(&clientMessage.Msg) {
			raft.StateLock.Unlock()
			log.Printf("I'm a leader..responding to read request...\n")
			typ, res := business_logic.ProcessMsg(&clientMessage.Msg)
			message := common.Message{PrimaryType: "ok", SecType: typ, Request: res}
			clientMessage.Response <- &message
		} else {
			log.Printf("I'm a leader..waiting for a quorom for write request...\n")
			raft.LogNumLock.Lock()

			//IDEA:
			/*
			- append the log and inrcrement the number
			- wait for commit channel / die channel / client timeout
				- in the leader pinger, the append entry will send the latest log
				- if got a failure, send the whole log and try again (use code below)
				- when majority of nodes respond, respond here to the client
			- when getting a commit channel msg, process the request
			*/


			//append the message to the log and let the pinger distribute it
			raft.LogNum += 1
			logNum := raft.LogNum
			raft.MsgLog = append(raft.MsgLog, &common.AppendMessage{Term: term, LogNum: raft.LogNum, Msg: clientMessage})		
			//spin until log is consistent
			log.Printf("Appended log %d\n", raft.LogNum)
			raft.StateLock.Unlock()
			raft.LogNumLock.Unlock()
			log.Printf("Waiting for log: %d to get committed\n", logNum)
			raft.waitForAppendToGetCommitted(logNum)

			log.Printf("Got a quorom...responding to client\n")

			//send response to client
			message := common.Message{PrimaryType: "ok"}
			clientMessage.Response <- &message
			log.Printf("Sent response to client handler in INFRA\n")
		}
		
	} 
}

