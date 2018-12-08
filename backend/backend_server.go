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
//TODO: remove threads from the map when they die
//TODO: remove connections from the map when they die
//CONSIDER - have two connections for each node

//connMap is protected by RWMutex (one or more connection can be a front end)
//threadMap maps thread ids to Channels so they can accept messages

// every node starts dialing to all other nodes - it doesn't care when they fail
	// checks if remote Addr is in the connMap
	// it saves a map of remoteAddr -> conn

// every node is listening on the tcp port
	// every acceptance, it checks if the remoteAddr is in the connMap
	// is saves the conn to the map

// every time a connection is made, two threads start: 
	// a messageDispatcher continuously sends message from its SendQueue
	// a messageListener thread puts messsages in their threads channels (using threadMap)
	//

// when a task thread starts:
	// its id and Message channels are inserted to the threadMap

//(future) task threads can be:
	// hearbeat (if leader)
	// timeout (if non leader)
	// log keeping thread


//Raft Logic 
const LOGSIZE = 1000
const RAFTQUEUESIZE = 1000

type Raft struct {
	State int //0: follower, 1: candidate, 2: leader
	StateLock sync.Mutex
	Term int
	TermLock sync.Mutex // always lock this before the 
	MsgLog []*infra.Message
	MsgQueue chan *infra.Message
	PingChan chan *infra.RaftMessage
	LeaderId int
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

func (raft *Raft) raftManager() {
	log.Println("")

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
		log.Printf("RAFT: vote request has lower term than mine, dropping")
	}
}

//should only get started once node becomes leader
func (raft *Raft) leaderPinger() {
	me := infra.MakeThread()
	timer := time.NewTimer(raft.LeaderTimeout / 3 * time.Millisecond)
	var ping infra.RaftMessage
	for {
		select {
			case <-raft.LeaderPingerKiller:
				//whoever did this will change the states
				return
			default:
				ping = infra.RaftMessage{Term: raft.Term, Data: "ping"}
				infra.QueueMessageForAll(&infra.Message{Tid: me.Tid, PrimaryType: "raft", SecType: "leaderPing", Request: ping})
				<-timer.C 
				timer = time.NewTimer(raft.LeaderTimeout / 3 * time.Millisecond)
		}
	}
}

func (raft *Raft) leaderPingHandler(raftMsg *infra.RaftMessage) {
	//if got ping of higher term an am leader / in election - kill channels and become follower
	raft.TermLock.Lock()
	raft.StateLock.Lock()
	
	if raftMsg.Term > raft.Term { 
		if raft.State == 1 { //if candidate, kill elections
			raft.ElectionsKiller <- 1
		} else if raft.State == 2 { // if leader, relinquish
			raft.LeaderPingerKiller <- 1		}

		raft.Term = raftMsg.Term
		raft.TermLock.Unlock()
		raft.StateLock.Unlock()
		raft.becomeFollower() //will only be able to start after StateLock is released
		return
	} else if raftMsg.Term == raft.Term{
		raft.PingChan <- raftMsg
	}
	raft.TermLock.Unlock()
	raft.StateLock.Unlock()
	//if got ping of a lower term, drop it
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
	go raft.leaderPinger()
}

func (raft *Raft) msgHandler() {
	var message *infra.Message
	var raftMessage infra.RaftMessage
	for {
		select {
			case message = <-raft.MsgQueue:
				switch message.PrimaryType {
				case "raft":
					raftMessage = message.Request.(infra.RaftMessage)
					switch message.SecType {
					case "leaderPing":
						go raft.leaderPingHandler(&raftMessage)
					case "appendEntry":
						go raft.appendEntryHandler(&raftMessage)
					case "voteRequest":
						go raft.vote(message)
					} 
				case "client":
					go raft.clientMessageHandler(message)
				}
		}
	}
}

func (raft *Raft) clientMessageHandler(message *infra.Message) {
	//REMEMBER: if at any point node is not a leader anymore, stop and reply with the leader's address 
	clientMessage := message.Request.(infra.ClientMessage)
	log.Println(clientMessage)
	//first spin if I am a candidate
	raft.StateLock.Lock()
	if raft.State == 1 {
		raft.StateLock.Unlock()
		for {
			raft.StateLock.Lock()
			if raft.State != 1 {
				break
			}
			raft.StateLock.Unlock()
		}
		raft.StateLock.Unlock()
	}

	//if I am not the leader, tell the client who the leader is
	
	if raft.State == 2 {
		//respond with the leader's address
	} else  {
		//spin until I'm in a different state
	}
	
}
func (raft *Raft) appendEntryHandler(entry *infra.RaftMessage) {
	//TODO: if I am a candidate, wait to get a signal from the raft channel

	//TODO: if I am not the leader, respond with the leader's address

	//TODO: if I am the leader:
		//send append to everyone else
		//wait for their responses, if got a qourom, send commit
		//wait for their responses
	message := <- raft.MsgQueue
	log.Printf("Got Client Message: %#v\n", *message)
	

	//respond
	//response := Message{Tid: message.Tid, PrimaryType: "response", Request: message.Request}
	//infra.QueueMessage(nodeConn.Queue, &response)
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