package infra

import (
	"bufio"
	"log"
	"net"
	"encoding/gob"
	"time"
	"sync"
	"fmt"
	"sync/atomic"
	"io"
	"strconv"
	"../../common"
)

/*
	The purposes of this package are:
			1. To keep the connection to all nodes alive and to know which nodes are available
			2. To manage communications between thread on the same connection

	In a few words, if conn A wants to send a message to conn B, it doesnt care to which thread it sends it
	The thread handling the message on conn B will be determined by conn B's logic and the message type
	Responses however, get sent directly to the appropriate channel of the thread awaiting that response
*/

const DIALINTERVAL = 5 // seconds between calling each node
const QUEUESIZE = 10 // how many messages await in the queue of each node at a time
const RESPONSESSIZE = 10 // how many responses await each thread at a given time

var connMap map[int]*NodeConnection //connection id -> connection
var connMapLock sync.RWMutex // protects connMap

var threadMap map[int32]*Thread //tracks the threads by their id
var threadMapLock sync.RWMutex

var msgChan chan common.Message // Where all non-response messages go (handled by the raft package)

var id int
var tid int32

var clusterSize int

type NodeConnection struct {
	nodeAddr string 
	reader *bufio.Reader
	writer *bufio.Writer
	Queue chan *common.Message
	QueueLock sync.RWMutex
	listenThreadId int32
	dispatchTheadId int32
	Id int
	Encoder *gob.Encoder
	Decoder *gob.Decoder
}


type Thread struct {
	Tid int32
	KillChan chan int
	Responses chan *common.Message
}

//get this node's id
func GetId() int {
	return id
}

//get the number of nodes in this cluster
func GetClusterSize() int {
	return clusterSize
}

//Thread Communication Infra
//==========================

//creates a thread and adds it to the map
func MakeThread() *Thread {
	thread := Thread{}
	thread.Tid = atomic.AddInt32(&tid, 1)
	thread.KillChan = make(chan int, 1)
	thread.Responses = make(chan *common.Message, RESPONSESSIZE) //this is how the thread will await responses
	insertThread(&thread)
	return &thread
}

//get a connection from its id
func GetConn(id int) *NodeConnection {
	if conn, found := connMap[id]; found {
		return conn
	}
	return nil
}


//insert a thread to the map
func insertThread(thread *Thread) {
	threadMapLock.Lock()
	defer threadMapLock.Unlock()
	threadMap[thread.Tid] = thread
}

//removes a thread from the map
func RemoveThread(id int32) {
	threadMapLock.Lock()
	defer threadMapLock.Unlock()
	delete(threadMap, id)
}

//get the thread pointer from its id
func getThread(id int32) *Thread {
	threadMapLock.RLock()
	defer threadMapLock.RUnlock()
	t, ok := threadMap[id] 
	if !ok {
		log.Println("trying to get non existing thread..")
		return nil
	}
	return t
}

//this method is called as a goroutine on each node connection
//Takes messages on the Nodes queue and sends them over the network
func dispatchMessages(nodeConn *NodeConnection) {
	log.Printf("starting dispatch thread for node %s\n", nodeConn.nodeAddr)
	me := MakeThread()
	defer RemoveThread(me.Tid)
	nodeConn.dispatchTheadId = me.Tid
	queue := nodeConn.Queue
	var message *common.Message
	var err error
	for {
		select {
	        case <- me.KillChan:
	        	log.Printf("Node %s is gone, stopping dispatching \n", nodeConn.nodeAddr)
	        	//TODO: if die, put failures in the right threads
	            return
	        case message = <- queue:
	        	//if fails, put a failure in accepqueue
	        	err = nodeConn.sendMessage(message)	
	        	if err != nil {
	        		log.Printf("Could not dispatch message %#v, sending failure instead: %#v", *message, err)
	        		failureResponse := common.Message{Tid: message.Tid, PrimaryType: "response", SecType: "failure"}
	        		getThread(message.Tid).Responses <- &failureResponse
	        	}
        }
	}
}

//this method is called as a goroutine on each node connection
//Decodes messages from the TCP connection and sends them to the right thread
//reponses are sent to the thread with the id specified in the message
//raft and client messages are sent to the raft handler
//id messages (used between nodes to tell each other which id they got) are handled here
func acceptMessages(nodeConn *NodeConnection) {
	log.Printf("starting accept thread for node %s\n", nodeConn.nodeAddr)
	//get Message and put in threads channels here
	me := MakeThread()
	defer RemoveThread(me.Tid)
	nodeConn.listenThreadId = me.Tid
	var message common.Message
	var thread *Thread
	for {
		select {
			case <- me.KillChan:
				log.Printf("Node %s is gone, stopping listening \n", nodeConn.nodeAddr)
	        	//TODO: if die, think what to do with the messages
	            return
	        default:
	        	dec := nodeConn.Decoder
	        	err := dec.Decode(&message)
	        	if err != nil {
	        		if err == io.EOF {
	        			log.Printf("Node %s closed the connection (reached EOF)\n", nodeConn.nodeAddr)
	        			cleanConn(nodeConn)
        			} else {
        				log.Println("Error Decoding: ", err)
        			}
				} else {
					err = nodeConn.writer.Flush()
					if err != nil {
						panic("Flush error") 
					}
					//message logic here
					message.NodeId = nodeConn.Id
					if message.PrimaryType == "response" {
						thread = getThread(message.Tid)
						if thread == nil {
							log.Printf("non existing thread destination for message %#v, dropping", message)
						} else {
							thread.Responses <- &message
						}
						
					} else if message.PrimaryType == "raft" || message.PrimaryType == "client" { //TODO: get rid of client, it should never get here
						message.NodeAddr = nodeConn.nodeAddr
						message.NodeId = nodeConn.Id
						if message.SecType == "fullAppend" {
							log.Printf("INFRA: Got fullAppend")
						}
						msgChan <- message
						if message.SecType == "fullAppend" {
							log.Printf("INFRA: entry in the channel")
						}
					} else if message.PrimaryType == "id" {
						nodeId, ok := message.Request.(int)
						if !ok {
							panic("got an invalid id message")
						}
						log.Printf("got id: %d\n", nodeId)
						nodeConn.Id = nodeId
					} else {
						log.Printf("Message type error: Dropping message %#v", message)
					}
					
				}
		}
		
	}
	
}

//Client Communication
//====================

//Takes a client message and sends it to the Raft channel, waits for a response,  and returns
//doesn't handle timeouts, both raft and the frontend know to time out
func raftClientProcessRequest(message *common.Message) (*common.Message) {
	if message.SecType == "Id" { //respond to Id requests immediately
		return &common.Message{PrimaryType: "Id", Request: id}
	}
	clientMessage, ok := message.Request.(common.ClientMessage)
	if !ok {
		log.Printf("Can't make sense of client request, dropping")
		return nil
	}

	//prepare the message
	clientMessage.Response = make(chan *common.Message, 1)
	message.Request = clientMessage
	msgChan <- *message

	//wait for the response
	response := <-clientMessage.Response
	log.Printf("Sending response: %#v\n", response)
	return response
}

//this function is called as a gorouting which runs until the frontend is gone
//it is called from the createConn function when it discovers that the called is a frontend
//it is called with the message sent by the front end and responds to all subsequent messages
func clientMessageHandler(conn net.Conn, nodeConn *NodeConnection,firstMessage *common.Message) {
	
	log.Println("Got a new client connection...")
	//process first message
	response := raftClientProcessRequest(firstMessage)
	sendMessageToClient(nodeConn, response)

	//start listening
	var err error
	request := &common.Message{}
	for {
		err = nodeConn.Decoder.Decode(request)
		if err != nil {
			if err == io.EOF {
				log.Println("The connection was closed by the client")
				return
			} else {
				log.Println("Error decoding GOB data:", err)
				return
			}
			response := raftClientProcessRequest(request)
			sendMessageToClient(nodeConn, response)
		}
	}
}

//takes the client connection and the message and handles the sending

func sendMessageToClient(nodeConn *NodeConnection, message *common.Message) {
	log.Printf("sending message to client: %#v\n", message)
	err := nodeConn.Encoder.Encode(*message)
	if err != nil {
		//TODO: if error means the thread is gone, kill it
		log.Printf("Could not encode message to client: %#v\n", err)
		return 
	}
	err = nodeConn.writer.Flush()
	if err != nil {
		log.Printf("%#v: Flush failed.\n", err)
		return
	}
}

//sends a message over the connection on which its called
//this is only called by the createConn function and the messages dispatcher
func (nodeConn *NodeConnection) sendMessage(message *common.Message) (error) {	
	enc := nodeConn.Encoder
	err := enc.Encode(*message) //marshall the request
	if err != nil {
		//TODO: if error means the thread is gone, kill it
		return fmt.Errorf("%#v: Encode failed for struct: %#v", err, *message)
	}
	err = nodeConn.writer.Flush()
	if err != nil {
		return fmt.Errorf("%#v: Flush failed.", err)
	}

	if message.SecType == "fullAppend" {
		log.Printf("Infra: Append sent to node %d\n", nodeConn.Id)
	}
	return nil
}


//queues a messages into a connection queue
func QueueMessage(queue chan<- *common.Message, message *common.Message) {
	queue <- message
}


//queues a message for all connections
func QueueMessageForAll(message *common.Message) {
	connMapLock.RLock()
	defer connMapLock.RUnlock()
	for _, nodeConn := range(connMap) {
		nodeConn.Queue <- message
	}
	
}

//Connection handlers
//===================

//deletes a connection from the map and stops its acceptor and dispatcher
func cleanConn(nodeConn *NodeConnection) {
	getThread(nodeConn.listenThreadId).KillChan <- 1
	getThread(nodeConn.dispatchTheadId).KillChan <- 1
	connMapLock.Lock()
	delete(connMap, nodeConn.Id)
	connMapLock.Unlock()
	go DialNode(nodeConn.nodeAddr)
}



//initialize a connection object
func nodeConnInit(conn net.Conn) bool {
	//create connection if it doesnt exist
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	nodeConn := createConn(conn, reader, writer)
	if nodeConn != nil {
		log.Println("New connction with " + nodeConn.nodeAddr)
	}
	
	return true
}

//handles the creation of a nodeConnection, setting up its encoder and decoder, and id exchage
func createConn(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) *NodeConnection {
	nodeConn := NodeConnection{}
	nodeConn.reader = reader
	nodeConn.writer = writer
	nodeConn.nodeAddr = conn.RemoteAddr().String()
	nodeConn.Queue = make(chan *common.Message, QUEUESIZE)
	var err error
	
	//send and accept our nodeIds
	nodeConn.Encoder = gob.NewEncoder(nodeConn.writer)
	nodeConn.Decoder = gob.NewDecoder(nodeConn.reader)
	err = nodeConn.sendMessage(&common.Message{Tid: -1, PrimaryType: "id", Request: id})
	if err != nil {
		log.Printf("Error sending message: %#v", err)
	}
	var message common.Message
	dec := nodeConn.Decoder
	err = dec.Decode(&message)
	if err != nil {
		log.Printf("error decoding id message: %#v", err)
		return nil
	} else if message.PrimaryType == "client" {
		go clientMessageHandler(conn, &nodeConn, &message)
		return nil
	} else if message.PrimaryType != "id" {
		log.Printf("error decoding id: got a different message")
		return nil
	}
	nodeId, ok := message.Request.(int)
	if !ok {
		log.Printf("error converting id")
		return nil
	}
	log.Printf("Checking if connection already exists...Id: %d", nodeId)
	connMapLock.Lock()
	defer connMapLock.Unlock()
	if _, ok := connMap[nodeId]; ok { //check if the node is already
		return nil
	}
	connMap[nodeId] = &nodeConn
	nodeConn.Id = nodeId
	go dispatchMessages(&nodeConn)
	go acceptMessages(&nodeConn)
	return &nodeConn
}


//diar a node at a given address
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

//open a connection
func Open(addr string) (net.Conn, error) {
	log.Println("Dialing " + addr + "..")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%#v: Dialing "+addr+" failed", err)
	}
	return conn, nil
}


//listen to incoming connections
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

//starts the infrastructure for node and communication management.
//takes this node's id, the port, backends to connect to, and a channel (raft) to send messages to
func StartInfra(port string, backends []string, idstr string, defaultMsgChan chan common.Message) {
	//register types to be decoded
	gob.Register(common.RaftMessage{})
	gob.Register(common.AppendMessage{})
	gob.Register([]*common.AppendMessage{})
	gob.Register(common.ClientMessage{})
	gob.Register(common.CommitMessage{})
	gob.Register(map[int32]*common.Book{})
	gob.Register(common.Book{})
	

	//initialize globals
	connMap = make(map[int]*NodeConnection)
	threadMap = make(map[int32]*Thread)
	clusterSize = len(backends) + 1
	msgChan = defaultMsgChan

	//set up my id
	if i, err := strconv.Atoi(idstr); err == nil {
    	id = i
	} else {
		panic("invalid id argument")
	}
	go listen(port)
	for _, addr := range(backends) {
		go DialNode(addr)
	}
}