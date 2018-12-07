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

const DIALINTERVAL = 5
const QUEUESIZE = 10
const RESPONSESSIZE = 10

var connMap map[string]*NodeConnection
var connMapLock sync.RWMutex

var threadMap map[int32]*Thread
var threadMapLock sync.RWMutex

var msgChan chan *Message // where all messages that are not responses go

var id int
var tid int32

type NodeConnection struct {
	nodeAddr string
	reader *bufio.Reader
	writer *bufio.Writer
	Queue chan *Message
	QueueLock sync.RWMutex
	listenThreadId int32
	dispatchTheadId int32
	Id int
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



//Thread Communication Infra
func MakeThread() *Thread {
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
	t, ok := threadMap[id] 
	if !ok {
		log.Println("trying to get non existing thread..")
		return nil
	}
	return t
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
					} else if message.PrimaryType == "raft" || message.PrimaryType == "client" {
						message.NodeAddr = nodeConn.nodeAddr
						msgChan <- &message
					} else if  message.PrimaryType == "id"{
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


func QueueMessage(queue chan<- *Message, message *Message) {
	queue <- message
}


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
	QueueMessage(nodeConn.Queue, &Message{Tid: -1, PrimaryType: "raft", SecType: "id", Request: id})
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
	connMap = make(map[string]*NodeConnection)
	threadMap = make(map[int32]*Thread)
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