package main

import (
	"bufio"
	"log"
	"net"
	"encoding/gob"
	"os"
	"time"
	"sync"
	"fmt"
	"strings"
	"sync/atomic"
	"io"
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

const DIALINTERVAL = 5
const QUEUESIZE = 10
const RESPONSESSIZE = 10

var backends []string
var connMap map[string]*NodeConnection
var connMapLock sync.RWMutex

var threadMap map[int32]*Thread
var threadMapLock sync.RWMutex

var tid int32

type NodeConnection struct {
	nodeAddr string
	reader *bufio.Reader
	writer *bufio.Writer
	Queue chan *Message
	QueueLock sync.RWMutex
	listenThreadId int32
	dispatchTheadId int32
}

type Message struct {
	Tid int32
	PrimaryType string
	SecType string
	NodeAddr string
	Request interface{}
}

type Thread struct{
	Tid int32
	KillChan chan int
	Responses chan *Message
}

type RaftMessage struct {
	Type int
	data interface{}
}


//Thread Communication Infra
func makeThread() *Thread {
	thread := Thread{}
	thread.Tid = atomic.AddInt32(&tid, 1)
	thread.KillChan = make(chan int, 1)
	thread.Responses = make(chan *Message, RESPONSESSIZE)
	insertThread(&thread)
	return &thread
}

func getConn(addr string) *NodeConnection {
	if conn, found := connMap[addr]; found {
		return conn
	}
	return nil
}
func insertThread(thread *Thread) {
	threadMapLock.Lock()
	defer threadMapLock.Unlock()
	threadMap[thread.Tid] = thread
}

func removeThread(id int32) {
	threadMapLock.Lock()
	defer threadMapLock.Unlock()
	delete(threadMap, id)
}

func getThread(id int32) *Thread {
	threadMapLock.RLock()
	defer threadMapLock.RUnlock()
	if t, ok := threadMap[id]; ok {
		return t
	}
	log.Println("trying to get non existing thread..")
	return nil
}

func dispatchMessages(nodeConn *NodeConnection) {
	log.Printf("starting dispatch thread for node %s\n", nodeConn.nodeAddr)
	me := Thread{Tid: atomic.AddInt32(&tid, 1), KillChan: make(chan int, 1), Responses: nil}
	nodeConn.dispatchTheadId = me.Tid
	insertThread(&me)
	queue := nodeConn.Queue
	var message *Message
	for {
		select {
	        case <- me.KillChan:
	        	log.Printf("Node %s is gone, stopping dispatching \n", nodeConn.nodeAddr)
	        	//TODO: if die, put failures in the right threads
	            removeThread(me.Tid)
	            return
	        case message = <- queue:
	        	//if fails, put a failure in accepqueue
	        	nodeConn.sendMessage(message)	
        }
	}
}

func acceptMessages(nodeConn *NodeConnection) {
	log.Printf("starting accept thread for node %s\n", nodeConn.nodeAddr)
	//get Message and put in threads channels here
	me := Thread{Tid: atomic.AddInt32(&tid, 1), KillChan: make(chan int, 1), Responses: nil}
	nodeConn.listenThreadId = me.Tid
	insertThread(&me)
	var message Message
	var thread *Thread
	for {
		dec := gob.NewDecoder(nodeConn.reader)
		select {
			case <- me.KillChan:
				log.Printf("Node %s is gone, stopping listening \n", nodeConn.nodeAddr)
	        	//TODO: if die, think what to do with the messages
	        	removeThread(me.Tid)
	            return
	        default:
	        	err := dec.Decode(&message)
	        	if err != nil {
	        		if err == io.EOF {
	        			log.Printf("Node %s closed the connection (reached EOF)\n", nodeConn.nodeAddr)
	        			cleanConn(nodeConn)
        			} else {
        				log.Println("Error Decoding: ", err)
        			}
				} else {
					//message logic here
					thread = getThread(message.Tid)
					if message.PrimaryType == "response" {
						thread.Responses <- &message
						log.Println(message)
					} else if message.PrimaryType == "raft" {

					} else if message.PrimaryType == "client" {
						go clientMessageHandler(nodeConn, &message)
					} else {
						log.Printf("Message type error: Dropping message %#v", message)
					}
					
				}
		}
		
	}
	
}

func (nodeConn *NodeConnection) sendMessage(message *Message) (error) {
	
	log.Printf("Sending: \n%#v\n", message)
	
	enc := gob.NewEncoder(nodeConn.writer)
	err := enc.Encode(*message) //marshall the request
	if err != nil {
		//TODO: if error means the thread is gone, kill it
		return fmt.Errorf("%#v: Encode failed for struct: %#v", err, *message)
	}
	err = nodeConn.writer.Flush()
	if err != nil {
		return fmt.Errorf("%#v: Flush failed.",err)
	}
	log.Println("Message Sent!")
	return nil
}


func queueMessage(queue chan<- *Message, message *Message) {
	queue <- message
}


//Raft Logic 

func ExecuteRaftMessage(message *Message) {
	log.Println("H")
}

func clientMessageHandler(nodeConn *NodeConnection, message *Message) {
	//TODO: if I am a candidate, wait to get a signal from the raft channel

	//TODO: if I am not the leader, respond with the leader's address

	log.Printf("Got Client Message: %#v\n", *message)
	

	//respond
	response := Message{Tid: message.Tid, PrimaryType: "response", Request: message.Request}
	queueMessage(nodeConn.Queue, &response)
}


//keep a state and an election term
//thread for timing out on the leader, dies when the time times out and goes to elections
//thread for going to elections - handling vote requests and counting
//thread for sending leader pings to everyone

//Business Logic

//Connection handlers

func cleanConn(nodeConn *NodeConnection) {
	getThread(nodeConn.listenThreadId).KillChan <- 1
	getThread(nodeConn.dispatchTheadId).KillChan <- 1
	print("Sent kill message")
	connMapLock.Lock()
	delete(connMap, nodeConn.nodeAddr)
	connMapLock.Unlock()
	//TODO: remove node from the map
	go DialNode(nodeConn.nodeAddr)
}


func nodeConnInit(conn net.Conn) bool {
	//create connection if it doesnt exist
	//false -> failed to connect
	//true -> connection exists or successful

	var nodeConn *NodeConnection
	connMapLock.Lock()
	defer connMapLock.Unlock()
	nodeAddr := conn.RemoteAddr().String()
	nodeConn = getConn(nodeAddr)
	if nodeConn != nil {
		 return true
	}
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	createConn(nodeAddr, reader, writer)
	log.Println("Connected to " + nodeAddr)
	return true
}

func createConn(addr string, reader *bufio.Reader, writer *bufio.Writer) *NodeConnection {
	nodeConn := NodeConnection{}
	nodeConn.reader = reader
	nodeConn.writer = writer
	nodeConn.nodeAddr = addr
	nodeConn.Queue = make(chan *Message, QUEUESIZE)
	connMap[nodeConn.nodeAddr] = &nodeConn
	go dispatchMessages(&nodeConn)
	go acceptMessages(&nodeConn)
	return &nodeConn
}

func DialNode(nodeAddr string) {
	// keep trying to connect forever (serve as ping-ack as well)
	timer := time.NewTimer(DIALINTERVAL * time.Second)
	for {
		conn, err := Open(nodeAddr)
		if err == nil && nodeConnInit(conn) {
			return
		} else {
			// TODO: put a kill message in channel
			// delete the connection
		}
		<-timer.C	
	}
}

func Open(addr string) (net.Conn, error) {
	log.Println("Dialing " + addr + "..")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%#v: Dialing "+addr+" failed", err)
	}
	return conn, nil
}

func listen(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Println("can't listen: ", err)
		return fmt.Errorf("%#v: Unable to listen on port %s\n", err, port)
	}
	log.Println("Listen on", listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed accepting a connection request:", err)
			continue
		}

		log.Printf("%s trying to connect\n", conn.RemoteAddr().String())
		nodeConnInit(conn)

	}
}

func setup(args []string) {
	port := ":61000"
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
		} else if expectedArg == "backend" {
			addresses := strings.Split(arg, ",")
			fmt.Println(addresses)
			for _, addr := range addresses {
				if addr[0] == ':' {
					addr = defaultHostname + addr
					backends = append(backends, addr)
					go DialNode(addr)
				} else {
					backends = append(backends, addr)
					go DialNode(addr)
				}
				expectedArg = ""
			}
		} else if expectedArg == "listen" {
			port = ":" + arg
			expectedArg = ""
		} else {
			panic("command line error")	
		}
		i++
	}
	go listen(port)
}


func messageThread() {
	me := makeThread()
	types := []string{"A", "B", "C", "D"}
	var message Message
	var response *Message
	for _, conn := range connMap {
		for _,t := range types {
			message = Message{Tid: me.Tid, PrimaryType: "client", Request: t}
			queueMessage(conn.Queue, &message)
			response = <- me.Responses
			log.Printf("Thread %d got response: %#v\n", me.Tid, *response)
		}
	}
}

func main() {
	//fetch command line arguments
	args := os.Args[1:]
	connMap = make(map[string]*NodeConnection)
	threadMap = make(map[int32]*Thread)
	wg := sync.WaitGroup{}
	wg.Add(1)
	setup(args)
	time.Sleep(5 * time.Second)
	//go messageThread()
	wg.Wait()

}