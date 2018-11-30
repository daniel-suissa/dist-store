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
)

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

var connMap map[string]*NodeConnection
var connMapLock sync.RWMutex

var threadMap map[int]*chan Message
var threadMapLock sync.RWMutex

var tid int32

type NodeConnection struct {
	nodeAddr string
	reader *bufio.Reader
	writer *bufio.Writer
	Queue chan *Message
	Lock sync.RWMutex
}

type Message struct {
	Tid int32
	Type string
	ConnId int
	Request interface{}
}

type RaftMessage struct {
	Type int
	data interface{}
}

func getConn(addr string) *NodeConnection {
	if conn, found := connMap[addr]; found {
		return conn
	}
	return nil
}

func insertConn(nodeConn *NodeConnection) {
	connMap[nodeConn.nodeAddr] = nodeConn
}
func dispatchMessages(nodeConn *NodeConnection) {
	queue := nodeConn.Queue
	var message *Message
	for {
		message = <- queue
		nodeConn.sendMessage(message)
	}
}

func acceptMessages(nodeConn *NodeConnection) {
	//get Message and put in threads channels here
	var message Message
	for {
		dec := gob.NewDecoder(nodeConn.reader)
		err := dec.Decode(&message)
		if err != nil {
			log.Println("Error Decoding: ", err)
		}
	}
	
}

func queueMessage(queue chan<- *Message, message *Message) {
	queue <- message
}

func nodeConnInit(nodeAddr string, getStream func(string) (*bufio.Reader, *bufio.Writer ,error) ) bool {
	var conn *NodeConnection
	connMapLock.Lock()
	defer connMapLock.Unlock()
	conn = getConn(nodeAddr)
	if conn != nil {
		 return true
	}

	reader, writer, err := getStream(nodeAddr) //open the connection and get a stream
	if err != nil {
		return false
	} else {
		createConn(nodeAddr, reader, writer)
		return true
	}
}
func createConn(addr string, reader *bufio.Reader, writer *bufio.Writer) *NodeConnection {
	nodeConn := NodeConnection{}
	nodeConn.reader = reader
	nodeConn.writer = writer
	nodeConn.nodeAddr = addr
	nodeConn.Queue = make(chan *Message, QUEUESIZE)
	insertConn(&nodeConn)
	go dispatchMessages(&nodeConn)
	go acceptMessages(&nodeConn)
	return &nodeConn
}

func DialNode(nodeAddr string) bool {
	timer := time.NewTimer(DIALINTERVAL * time.Second)
	for {
		if nodeConnInit(nodeAddr, Open) {
			return true
		}
		<-timer.C
		
	}
	return false
}

func Open(addr string) (*bufio.Reader, *bufio.Writer , error) {
	log.Println("Dial " + addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("%#v: Dialing "+addr+" failed", err)
	}
	return bufio.NewReader(conn), bufio.NewWriter(conn), nil
}

func (nodeConn *NodeConnection) sendMessage(message *Message) (error) {
	
	log.Printf("Sending: \n%#v\n", message)
	
	enc := gob.NewEncoder(nodeConn.writer)
	err := enc.Encode(*message) //marshall the request
	if err != nil {
		return fmt.Errorf("%#v: Encode failed for struct: %#v", err, *message)
	}
	err = nodeConn.writer.Flush()
	if err != nil {
		return fmt.Errorf("%#v: Flush failed.",err)
	}
	log.Println("Message Sent!")
	return nil
}

func listen(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("%#v: Unable to listen on port %s\n", err, port)
	}
	log.Println("Listen on", listener.Addr().String())
	for {
		log.Println("Accept a connection request.")
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed accepting a connection request:", err)
			continue
		}
		nodeConnInit(conn.RemoteAddr().String(), func (s string) (*bufio.Reader, *bufio.Writer ,error) {
				return bufio.NewReader(conn), bufio.NewWriter(conn), nil
			})

	}
}

func messageThread(tid int32) {
	types := []string{"A", "B", "C", "D"}
	//responseCh := make(chan *Message)
	var message Message
	for _, conn := range connMap {
		for _,t := range types{
			message = Message{Tid: tid, Type: t}
			queueMessage(conn.Queue, &message)
		}
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
			expectedArg = "backend"
		} else if expectedArg == "backend" {
			addresses := strings.Split(arg, ",")
			for _, addr := range addresses {
				if addr[0] == ':' {
					go DialNode(defaultHostname + arg)
				} else {
					go DialNode(arg)
				}
				expectedArg = ""
			}
		} else if expectedArg == "listen" {
			port = ":" + args[i+1]
			expectedArg = ""
		} else {
			panic("command line error")	
		}
		i++
	}
	go listen(port)
}

func main() {
	//fetch command line arguments
	args := os.Args[1:]
	setup(args)

}