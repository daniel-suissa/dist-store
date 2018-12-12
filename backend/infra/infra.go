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

var msgChan chan *common.Message // where all messages that are not responses go

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
	thread.Responses = make(chan *common.Message, RESPONSESSIZE)
	insertThread(&thread)
	return &thread
}

func GetConn(id int) *NodeConnection {
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

func RemoveThread(id int32) {
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
	        		log.Printf("Could not send message %#v, sending failure instead: %#v", *message, err)
	        		failureResponse := common.Message{Tid: message.Tid, PrimaryType: "response", SecType: "failure"}
	        		QueueMessage(nodeConn.Queue, &failureResponse)
	        	}
        }
	}
}

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
						} 
						thread.Responses <- &message
						
					} else if message.PrimaryType == "raft" || message.PrimaryType == "client" { //TODO: get rid of client, it should never get here
						message.NodeAddr = nodeConn.nodeAddr
						message.NodeId = nodeConn.Id
						msgChan <- &message
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
	msgChan <- message
	response := <-clientMessage.Response
	log.Printf("Sending response: %#v\n", response)
	return response
}

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

func sendMessageToClient(nodeConn *NodeConnection, message *common.Message) {
	log.Printf("sending message to client: %#v\n", message)
	err := nodeConn.Encoder.Encode(*message)
	if err != nil {
		//TODO: if error means the thread is gone, kill it
		log.Println("Could not write message to client")
		return 
	}
	err = nodeConn.writer.Flush()
	if err != nil {
		log.Printf("%#v: Flush failed.\n", err)
		return
	}
}

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
	return nil
}


func QueueMessage(queue chan<- *common.Message, message *common.Message) {
	queue <- message
}

func QueueMessageForAll(message *common.Message) {
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
	nodeConn.Queue = make(chan *common.Message, QUEUESIZE)
	var err error
	//send and accept the nodeId
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
		//TODO: pass the whole nodeConn here
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
	var message *common.Message
	var response *common.Message
	for _, conn := range connMap {
		for _,t := range types {
			message = &common.Message{Tid: me.Tid, PrimaryType: "client", Request: t}
			QueueMessage(conn.Queue, message)
			response = <- me.Responses
			log.Printf("Thread %d got response: %#v\n", me.Tid, *response)
		}
	}
}

func StartInfra(port string, backends []string, idstr string, defaultMsgChan chan *common.Message) {
	gob.Register(common.RaftMessage{})
	gob.Register(common.AppendMessage{})
	gob.Register([]*common.AppendMessage{})
	gob.Register(common.ClientMessage{})
	gob.Register(common.CommitMessage{})
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