package main

import (
	"./infra"
	"log"
	"os"
	"time"
	"sync"
	"fmt"
	"strings"
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



type RaftMessage struct {
	Type int
	data interface{}
}

//Raft Logic 
const LOGSIZE = 1000
const LEADERTIMEOUT = 3
const RAFTQUEUESIZE = 1000


type Raft struct {
	State int //0: follower, 1: candidate, 2: leader
	StateLock sync.Mutex
	Term int
	TermLock sync.Mutex
	MsgLog []*infra.Message
	MsgQueue chan *infra.Message
}

func initRaft() Raft {
	raft := Raft{State: 0, Term: 0, MsgQueue: make(chan *infra.Message, RAFTQUEUESIZE)}
	go raft.msgHandler()
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
	timer := time.NewTimer(LEADERTIMEOUT * time.Second)
	for {
		//get a ping message
		<-timer.C 
	}
}
func (raft *Raft) leaderPinger() {
	print("H")
}

func (raft *Raft) msgHandler() {
	var message *infra.Message
	for {
		select {
			case message = <-raft.MsgQueue:
				switch message.PrimaryType {
				case "raft":
					switch message.SecType {
					case "id":

					}
				}
		}
	}
}
func (raft *Raft) executeRaftMessage(message *infra.Message) {
	log.Println("H")
}

func (raft *Raft) clientMessageHandler() {
	//TODO: if I am a candidate, wait to get a signal from the raft channel

	//TODO: if I am not the leader, respond with the leader's address

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