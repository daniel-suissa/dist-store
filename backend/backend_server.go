package main

import (
	"./infra"
	"log"
	"os"
	"time"
	"sync"
	"fmt"
	"strings"
	"math/rand"
)

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


//Raft Logic 
const LOGSIZE = 1000
const RAFTQUEUESIZE = 1000

type Raft struct {
	//lock should only be locked in this order: Term, State, Log
	State int //0: follower, 1: candidate, 2: leader
	StateLock sync.Mutex
	Term int
	TermLock sync.Mutex
	MsgLog []*infra.AppendMessage
	LogNum int
	LogNumLock sync.Mutex
	LogConsistencyFlag bool // also controlled by the same mutex

	MsgQueue chan *infra.Message
	PingChan chan *infra.RaftMessage
	LeaderId int
	LeaderIdLock sync.Mutex
	ElectionsKiller chan int

	//if leader
	LeaderTimeout time.Duration
	ElectionTimeout time.Duration
	LeaderPingerKiller chan int

}

func initRaft() Raft {
	timeout := time.Duration(rand.Intn(150) + 150)
	raft := Raft{State: 0, Term: 0, MsgQueue: make(chan *infra.Message, RAFTQUEUESIZE), PingChan: make(chan *infra.RaftMessage, 1)}
	raft.LeaderTimeout = timeout
	timeout = time.Duration(rand.Intn(150) + 150)
	raft.ElectionTimeout = timeout
	raft.Term = 0
	raft.LogNum = 0
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
	raft.LeaderIdLock.Lock()
	raft.LeaderId = newLeader
	raft.LeaderIdLock.Unlock()
}

func (raft *Raft) leaderTimer () {
	log.Printf("RAFT: started leader timer")
	timer := time.NewTimer(raft.LeaderTimeout * time.Millisecond)
	var message *infra.RaftMessage
	for {
		select {
		case message = <-raft.PingChan:
			//check term number
			if message.Term >= raft.Term {
				//got a valid Append entry 
				//accept this as my leader
				raft.Term = message.Term
				timer = time.NewTimer(raft.LeaderTimeout * time.Millisecond)
				}
			//else just ignore this ping
		case <-timer.C:
			print("timer ended")
			//go to elections
			go raft.toElection()
			return
		}
	}
}

func (raft *Raft) toElection() {
	raft.StateLock.Lock()
	raft.TermLock.Lock()

	raft.State = 1
	raft.Term += 1
	thisTerm := raft.Term
	voteRequest := infra.RaftMessage{Term: raft.Term}
	raft.StateLock.Unlock()
	raft.TermLock.Unlock()
	
	me := infra.MakeThread()
	votes := 1 // vote for myself
	neededVotes := infra.GetClusterSize() / 2 + 1

	infra.QueueMessageForAll(&infra.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "voteRequest", Request: voteRequest})
	timer := time.NewTimer(raft.ElectionTimeout * time.Millisecond)
	var message *infra.Message
	var vote infra.RaftMessage
	log.Printf("RAFT: starting new elections with term: %d", thisTerm)
	for {
		select {
			case message = <-me.Responses:
				if message.PrimaryType == "response" { //could also be a failure but we don't care
					vote = message.Request.(infra.RaftMessage)
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
				go raft.toElection()
				return
		}
	}
}

func (raft *Raft) vote(message *infra.Message) {
	raftMessage, ok := message.Request.(infra.RaftMessage)
	if !ok {
		log.Println("can't get raft message out of message")
		return
	}
	log.Printf("RAFT: got a vote request for term %d : %#v\n", raftMessage.Term ,*message)
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()
	if raftMessage.Term > raft.Term {
		//got an VoteRequest entry with larger term than mine
		//voteYes
		raft.Term = raftMessage.Term
		response := &infra.Message{Tid: message.Tid, PrimaryType: "response", Request: infra.RaftMessage{Term: raft.Term}}
		conn := infra.GetConn(message.NodeId)
		infra.QueueMessage(conn.Queue, response)
		log.Printf("RAFT: Voted")

	} else {
		log.Printf("RAFT: Vote request has lower term than mine, dropping")
	}
}

//should only get started once node becomes leader
func (raft *Raft) sendAppendEntries() {
	me := infra.MakeThread()
	timer := time.NewTimer(raft.LeaderTimeout / 3 * time.Millisecond)
	var votes int
	neededVotes := infra.GetClusterSize() / 2 + 1
	for {
		FirstFor:
		select {
			case <-raft.LeaderPingerKiller:
				//whoever did this will change the states
				return
			default:
				raft.TermLock.Lock()
				raft.LogNumLock.Lock()
				var logEntry *infra.AppendMessage
				if len(raft.MsgLog) > 0 {
					logEntry = raft.MsgLog[len(raft.MsgLog)-1]
				} else {
					logEntry = &infra.AppendMessage{}
				}
				
				raftMsg := infra.RaftMessage{Term: raft.Term, Data: logEntry}
				infra.QueueMessageForAll(&infra.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "appendEntry", Request: raftMsg})
				raft.TermLock.Unlock()
				raft.LogNumLock.Unlock()
				
				votes = 1 // vote for myself
				for {
					select {
					case response := <-me.Responses:
						if response.SecType == "inconsistentLog" {
							go raft.sendLogForRecovery(response.NodeId)
						} else if response.SecType == "appendOk" {
							votes += 1
							if votes >= neededVotes {
								raft.LogNumLock.Lock()
								raft.LogConsistencyFlag = true
								raft.LogNumLock.Unlock()
								break FirstFor
							}
						}
					case <-timer.C:
						timer = time.NewTimer(raft.LeaderTimeout / 3 * time.Millisecond)
						break FirstFor
					}

				}
				<-timer.C 
				timer = time.NewTimer(raft.LeaderTimeout / 3 * time.Millisecond)
		}
	}
}

func (raft *Raft) appendEntryHandler(message *infra.Message) {
	//this function assumes that only the leader (or someone that should be the leader) 
	//sends append messages

	//if got ping of higher term an am leader / in election - kill channels and become follower
	raftMsg, _ := message.Request.(infra.RaftMessage)
	raft.TermLock.Lock()
	raft.StateLock.Lock()
	raft.LogNumLock.Lock()
	defer raft.TermLock.Unlock()
	defer raft.StateLock.Unlock()
	defer raft.LogNumLock.Unlock()

	if raftMsg.Term > raft.Term { 
		if raft.State == 1 { //if candidate, kill elections
			raft.ElectionsKiller <- 1
		} else if raft.State == 2 { // if leader, relinquish
			raft.LeaderPingerKiller <- 1		}

		raft.Term = raftMsg.Term
		raft.setLeader(message.NodeId)
		go raft.becomeFollower() //will only be able to start after StateLock is released
		return
	} 
	if raftMsg.Term == raft.Term {
		
		appndMsg, _ := raftMsg.Data.(infra.AppendMessage)
		//if log entry is one larger than mine, append it and process
		if appndMsg.LogNum == raft.LogNum + 1 {
			raft.MsgLog = append(raft.MsgLog, &appndMsg) //apend to log
			
			//TODO: process

			//respond
			leaderConn := infra.GetConn(message.NodeId)
			infra.QueueMessage(leaderConn.Queue, &infra.Message{
					Tid: message.Tid,
					PrimaryType: "response",
					SecType: "appendOk"})

		} else if appndMsg.LogNum > raft.LogNum {
			//respond with recovery request
			leaderConn := infra.GetConn(message.NodeId)
			infra.QueueMessage(leaderConn.Queue, &infra.Message{
					Tid: message.Tid,
					PrimaryType: "response",
					SecType: "inconsistentLog"})
		} else if appndMsg.LogNum <= raft.LogNum {
			//TODO: truncate the log
		}
		raft.PingChan <- &raftMsg // notify the leaderTimer that a message is in
	}
	
	//if got ping of a lower term, drop it
}
func (raft *Raft) sendLogForRecovery(nodeId int) {
	raft.TermLock.Lock()
	raft.LogNumLock.Lock()
	defer raft.TermLock.Unlock()
	defer raft.LogNumLock.Unlock()
	conn := infra.GetConn(nodeId)
	msg := infra.Message{
			PrimaryType: "raft", 
			SecType: "logRecovery", 
			Request: infra.RaftMessage{
				Term: raft.Term,
				Data: raft.MsgLog}}
	infra.QueueMessage(conn.Queue, &msg)
}
func (raft *Raft) logRecovery(raftMsg * infra.RaftMessage) {
	log.Println("RAFT: Got log for recovery...")
	//TODO
}

func (raft *Raft) becomeFollower() {
	raft.StateLock.Lock()
	raft.State = 0
	raft.StateLock.Unlock()
	go raft.leaderTimer()
}

func (raft *Raft) becomeLeader() {
	raft.StateLock.Lock()
	raft.State = 2
	raft.StateLock.Unlock()
	go raft.sendAppendEntries()
}

func (raft *Raft) msgHandler() { //consider running many of there
	var message *infra.Message
	var raftMessage infra.RaftMessage
	for {
		select {
			case message = <-raft.MsgQueue:
				switch message.PrimaryType {
				case "raft":
					raftMessage = message.Request.(infra.RaftMessage)
					switch message.SecType {
					case "logRecovery":
						go raft.logRecovery(&raftMessage)
					case "appendEntry":
						go raft.appendEntryHandler(message)
					case "voteRequest":
						go raft.vote(message)
					} 
				case "client":
					go raft.clientMessageHandler(message) //TODO: get rid of this, it should never get here
				}
		}
	}
}

func (raft *Raft) clientMessageHandler(message *infra.Message) {
	//REMEMBER: if at any point node is not a leader anymore, stop and reply with the leader's address 
	clientMessage := message.Request.(infra.ClientMessage)
	log.Println(clientMessage)

	//first spin if I am a candidate
	raft.TermLock.Lock()
	defer raft.TermLock.Unlock()

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
		message := infra.Message{PrimaryType: "leaderId", Request: raft.getLeader()}
		clientMessage.Response <- &message

	} else { //I am the leader
		log.Printf("I'm a leader..waiting for a quorom...\n")
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
		raft.MsgLog = append(raft.MsgLog, &infra.AppendMessage{Term: raft.Term, LogNum: raft.LogNum, Msg: clientMessage})
		raft.LogNum += 1
		raft.LogConsistencyFlag = false
		
		//spin until log is consistent
		raft.LogNumLock.Unlock()
		for {
			raft.LogNumLock.Lock()
			if raft.LogConsistencyFlag {
				raft.LogNumLock.Unlock()
				break
			}
			raft.LogNumLock.Unlock()
		}
		log.Printf("Got a quorom...responding to client\n")
		//process the request (ask business logic)

		//send response to client
		message := infra.Message{PrimaryType: "ok"}
		clientMessage.Response <- &message
		
	} 
	raft.StateLock.Unlock()
	
}


//keep a state and an election term
//thread for timing out on the leader, dies when the time times out and goes to elections
//thread for going to elections - handling vote requests and counting
//thread for sending leader pings to everyone

//Business Logic

func parseCmdArgs(args []string) (string, []string, string){
	port := ":61000"
	var backendAddrs []string
	var id string
    defaultHostname := "127.0.0.1"
    var arg string
	i := 0
	expectedArg := "" //listen or backend
	for i < len(args) {
		arg = args[i]
		if arg == "--backend" {
			expectedArg = "backend"
		} else if arg == "--listen" && i + 1 < len(args) {
			expectedArg = "listen"
		} else if arg == "--id" { 
			expectedArg = "id"
		} else if expectedArg == "backend" {
			addresses := strings.Split(arg, ",")
			fmt.Println(addresses)
			for _, addr := range addresses {
				if addr[0] == ':' {
					addr = defaultHostname + addr
					backendAddrs = append(backendAddrs, addr)
				} else {
					backendAddrs = append(backendAddrs, addr)
				}
				expectedArg = ""
			}
		} else if expectedArg == "listen" {
			port = ":" + arg
			expectedArg = ""
		} else if expectedArg == "id" {
			id = arg 
		} else {
			panic("command line error")	
		}
		i++
	}
	return port, backendAddrs, id
}

func main() {
	//fetch command line arguments
	args := os.Args[1:]
	wg := sync.WaitGroup{}
	wg.Add(1)
	port, backendAddrs, id := parseCmdArgs(args)
	raft := initRaft()
	infra.StartInfra(port, backendAddrs, id, raft.MsgQueue)
	//go messageThread()
	wg.Wait()
}