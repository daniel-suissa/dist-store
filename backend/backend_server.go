package main

import (
	"bufio"
	"log"
	"net"
	"io"
	"encoding/gob"
	"os"
	"time"
	"sync"
	"fmt"
	"sync/atomic"
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

var connMap map[int]*NodeConnection

type NodeConnection struct {
	nodeAddr string
	rw *bufio.ReadWriter
	Queue chan Message
	Mutex sync.RWMutex
}

type Message struct {
	Tid int
	Type string
	ConnId int
	Request interface{}
}

type RaftMessage struct {
	Type int
	data interface{}
}

func dispatchMessages(nodeId int) {
	nodeConn = connMap[nodeId]
	queue = nodeConn.Queue
	var message Message
	for {
		message <- queue
		nodeConn.sendMessage(message)
	}
}

func acceptMessages(nodeId int) {

}

func queueMessage(queue chan<- Message, message Message) {
	queue <- message
}

func nodeConnInit(nodeAddr string) (*nodeConnection) {
	rw, err := Open(nodeAddr) //open the connection and get a stream
	if err != nil {
		panic(fmt.Sprintf("Client: Failed to open connection to %v: %v",serverAddr,err))
	}
	
	if err != nil {
		panic(fmt.Sprintf("Could not write GOB data: %v",serverAddr,err))
	}

	nodeConn := NodeConnection{}
	nodeConn.rw = rw
	nodeConn.serverAddr = serverAddr
	return &nodeConn
}

func Open(addr string) (*bufio.ReadWriter, error) {
	log.Println("Dial " + addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%#v: Dialing "+addr+" failed", err)
	}
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func (nodeConn *NodeConnection) sendMessage(message Message) (error) {
	
	log.Printf("Sending: \n%#v\n", message)
	
	enc := gob.NewEncoder(nodeConn.rw)
	err := enc.Encode(message) //marshall the request
	if err != nil {
		return fmt.Errorf("%#v: Encode failed for struct: %#v", err, request)
	}
	err = nodeConn.rw.Flush()
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
		go handleCmd(conn)
	}
}
func setup(args []string) {
	port := ":61000"
    defaultHostname := "127.0.0.1"
    var arg string
	i := 0
	nodeId := 0
	expectedArg = "" //listen or backend
	for i < len(args) {
		arg = args[i]
		if arg == "--backend" {
			expectedArg = "backend"
		} else if arg == "--listen" && i + 1 < len(args) {
			expectedArg = "backend"
		} else if cmd == "backend" {
			addresses := strings.Split(arg, ",")
			for addr := range addresses {
				if addr[0] == ":" {
					connMap[nodeId] = nodeConnInit(defaultHostname + arg)
				} else {
					connMap[nodeId] = nodeConnInit(arg)
				}
				cmd = ""
			}
		} else if cmd == "listen" {
			port = ":" + args[i+1]
			cmd = ""
		} else {
			panic("command line error")	
		}
		i++
	}
}

func main() {
	//fetch command line arguments
	args := os.Args[1:]
	setup(args)

}