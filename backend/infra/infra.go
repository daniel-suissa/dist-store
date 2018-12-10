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
)
/* important checks
	- all threads that register themselves should either remove themselves or go forever

*/
const DIALINTERVAL = 5
const QUEUESIZE = 10
const RESPONSESSIZE = 10

var connMap map[int]*NodeConnection
var connMapLock sync.RWMutex

var threadMap map[int32]*Thread
var threadMapLock sync.RWMutex

var msgChan chan *Message // where all messages that are not responses go

var id int
var tid int32

var clusterSize int

type NodeConnection struct {
	nodeAddr string
	reader *bufio.Reader
	writer *bufio.Writer
	Queue chan *Message
	QueueLock sync.RWMutex
	listenThreadId int32
	dispatchTheadId int32
	Id int
	Encoder *gob.Encoder
	Decoder *gob.Decoder
}


type Message struct {
	Tid int32
	PrimaryType string
	SecType string
	NodeAddr string
	NodeId int
	Request interface{}
}

type Thread struct {
	Tid int32
	KillChan chan int
	Responses chan *Message
}


type RaftMessage struct {
	Term int
	Data interface{}
}

type AppendMessage struct {
	Term int
	LogNum int
	Msg ClientMessage
}

type ClientMessage struct {
	Cmd string
	Book Book
	Conn net.Conn
	Response chan *Message
}

type Book struct {
	Id int
	Title string
	Author string
}

func GetId() int {
	return id
}

func GetClusterSize() int {
	return clusterSize
}
func GetConnMap() map[int]*NodeConnection{
	connMapLock.RLock()
	connMapCopy := connMap
	connMapLock.RUnlock()
	return connMapCopy
}
//Thread Communication Infra
func MakeThread() *Thread {
	thread := Thread{}
	thread.Tid = atomic.AddInt32(&tid, 1)
	thread.KillChan = make(chan int, 1)
	thread.Responses = make(chan *Message, RESPONSESSIZE)
	insertThread(&thread)
	return &thread
}

func GetConn(id int) *NodeConnection {
	log.Printf("INFRA: getting conn %d from map %#v", id, connMap)
	if conn, found := connMap[id]; found {
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
	t, ok := threadMap[id] 
	if !ok {
		log.Println("trying to get non existing thread..")
		return nil
	}
	return t
}

func dispatchMessages(nodeConn *NodeConnection) {
	log.Printf("starting dispatch thread for node %s\n", nodeConn.nodeAddr)
	me := MakeThread()
	nodeConn.dispatchTheadId = me.Tid
	queue := nodeConn.Queue
	var message *Message
	var err error
	for {
		select {
	        case <- me.KillChan:
	        	log.Printf("Node %s is gone, stopping dispatching \n", nodeConn.nodeAddr)
	        	//TODO: if die, put failures in the right threads
	            removeThread(me.Tid)
	            return
	        case message = <- queue:
	        	//if fails, put a failure in accepqueue
	        	err = nodeConn.sendMessage(message)	
	        	if err != nil {
	        		log.Printf("Could not send message %#v, sending failure instead: %#v", *message, err)
	        		failureResponse := Message{Tid: message.Tid, PrimaryType: "response", SecType: "failure"}
	        		QueueMessage(nodeConn.Queue, &failureResponse)
	        	}
        }
	}
}

func acceptMessages(nodeConn *NodeConnection) {
	log.Printf("starting accept thread for node %s\n", nodeConn.nodeAddr)
	//get Message and put in threads channels here
	me := MakeThread()
	nodeConn.listenThreadId = me.Tid
	var message Message
	var thread *Thread
	for {
		select {
			case <- me.KillChan:
				log.Printf("Node %s is gone, stopping listening \n", nodeConn.nodeAddr)
	        	//TODO: if die, think what to do with the messages
	        	removeThread(me.Tid)
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
							log.Printf("non existing thread destination for message %#v", message)
						}
						thread.Responses <- &message
						log.Println(message)
					} else if message.PrimaryType == "raft" || message.PrimaryType == "client" { //TODO: get rid of client, it should never get here
						message.NodeAddr = nodeConn.nodeAddr
						message.NodeId = nodeConn.Id
						msgChan <- &message
					} else if message.PrimaryType == "id"{
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

func raftClientProcessRequest(message *Message) (*Message) {
	
	if message.SecType == "Id" { //respond to Id requests immediately
		return &Message{PrimaryType: "Id", Request: id}
	}

	raftMessage, ok := message.Request.(RaftMessage)
	if !ok {
		log.Printf("Can't make sense of client request, dropping")
		return nil
	}

	clientMessage, ok := raftMessage.Data.(ClientMessage)
	if !ok {
		log.Printf("Can't make sense of client request, dropping")
		return nil
	}

	//prepare the message
	clientMessage.Response = make(chan *Message, 1)
	raftMessage.Data = clientMessage
	message.Request = raftMessage
	me := MakeThread()
	msgChan <- message
	response := <-me.Responses
	removeThread(me.Tid)
	return response
}

func clientMessageHandler(conn net.Conn, firstMessage *Message, dec *gob.Decoder, enc *gob.Encoder) {
	log.Println("Got a new client connection...")
	//process first message
	//response := raftClientProcessRequest(firstMessage)
	//sendMessageToClient(enc, response)

	//start listening
	var err error
	request := &Message{}
	for {
		err = dec.Decode(request)
		if err != nil {
			if err == io.EOF {
				log.Println("The connection was closed by the client")
				return
			} else {
				log.Println("Error decoding GOB data:", err)
				return
			}
			response := raftClientProcessRequest(request)
			sendMessageToClient(enc, response)
		}
	}
}

func sendMessageToClient(enc *gob.Encoder, message *Message) {
	err := enc.Encode(*message)
	if err != nil {
		//TODO: if error means the thread is gone, kill it
		log.Println("Could not write message to client")
		return 
	}
	if err != nil {
		log.Println("Could not write message to client")
		return
	}
}

func (nodeConn *NodeConnection) sendMessage(message *Message) (error) {	
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
	return nil
}


func QueueMessage(queue chan<- *Message, message *Message) {
	queue <- message
}

func QueueMessageForAll(message *Message) {
	connMapLock.RLock()
	defer connMapLock.RUnlock()
	for _, nodeConn := range(connMap) {
		nodeConn.Queue <- message
	}
	
}

//Connection handlers

func cleanConn(nodeConn *NodeConnection) {
	getThread(nodeConn.listenThreadId).KillChan <- 1
	getThread(nodeConn.dispatchTheadId).KillChan <- 1
	print("Sent kill message")
	connMapLock.Lock()
	delete(connMap, nodeConn.Id)
	connMapLock.Unlock()
	go DialNode(nodeConn.nodeAddr)
}


func nodeConnInit(conn net.Conn) bool {
	//create connection if it doesnt exist

	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	nodeConn := createConn(conn, reader, writer)
	if nodeConn != nil {
		log.Println("New connction with " + nodeConn.nodeAddr)
	}
	
	return true
}

func createConn(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) *NodeConnection {
	nodeConn := NodeConnection{}
	nodeConn.reader = reader
	nodeConn.writer = writer
	nodeConn.nodeAddr = conn.RemoteAddr().String()
	nodeConn.Queue = make(chan *Message, QUEUESIZE)
	var err error
	//send and accept the nodeId
	nodeConn.Encoder = gob.NewEncoder(nodeConn.writer)
	nodeConn.Decoder = gob.NewDecoder(nodeConn.reader)
	err = nodeConn.sendMessage(&Message{Tid: -1, PrimaryType: "id", Request: id})
	if err != nil {
		log.Printf("Error sending message: %#v", err)
	}
	var message Message
	dec := nodeConn.Decoder
	err = dec.Decode(&message)
	if err != nil {
		log.Printf("error decoding id message: %#v", err)
		return nil
	} else if message.PrimaryType == "client" {
		go clientMessageHandler(conn, &message, nodeConn.Decoder, nodeConn.Encoder)
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
		return connMap[nodeId]
	}
	connMap[nodeId] = &nodeConn
	nodeConn.Id = nodeId
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

func messageThread() {
	me := MakeThread()
	types := []string{"A", "B", "C", "D"}
	var message Message
	var response *Message
	for _, conn := range connMap {
		for _,t := range types {
			message = Message{Tid: me.Tid, PrimaryType: "client", Request: t}
			QueueMessage(conn.Queue, &message)
			response = <- me.Responses
			log.Printf("Thread %d got response: %#v\n", me.Tid, *response)
		}
	}
}

func StartInfra(port string, backends []string, idstr string, defaultMsgChan chan *Message) {
	gob.Register(RaftMessage{})
	gob.Register(AppendMessage{})
	gob.Register(ClientMessage{})
	connMap = make(map[int]*NodeConnection)
	threadMap = make(map[int32]*Thread)
	clusterSize = len(backends) + 1
	msgChan = defaultMsgChan
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