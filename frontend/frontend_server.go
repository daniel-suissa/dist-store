package main

import (
	//"github.com/kataras/iris"
	//"strconv"
	"os"
	"bufio"
	"log"
	"net"
	"encoding/gob"
	"fmt"
	"time"
	"sync"
	"strings"
	"../common"
	"github.com/kataras/iris"
	"strconv"

)


//front end starts off by asking the backends for their ids

//health check:
	//periodically dial the leader with message "are you the leader"
		//if getting a leaderId response, change leaders
		//if getting a yes reponse, health check successful
		
		//if request times out, print a message and try again
		//if the connection is down send an empty message to a different node

//Request:
	//send the message to the leader node (first leader node assumed to be of lowest Id)
	//response can either be (1) failure (can't reach qourom), (2) leaderId (new leader) or (3) ok + data
	//response can potentially time out, but only if the node hit is a candidate, in that case hit a different node

const HEALTHINTERVAL = 5
const RESPONSETIMEOUT = 600

type ServerConnection struct {
	serverAddr string
	id int
	enc *gob.Encoder
	dec *gob.Decoder
	rw *bufio.ReadWriter
	conn *net.Conn
}

//temporary for compilation
type Request struct {
	Cmd string // getall/getone/add/update/delete
	Book common.Book
}


var addrToId map[string]int
var idToAddr map[int]string

var backendsLock sync.Mutex
var leaderAddr string

var leaderLock sync.Mutex
var backends []string


//id counter to create unique ids (since all records are in memory it is assumed an integer is enough)
var idCounter int


func getIdFromAddr(addr string) int {
	backendsLock.Lock()
	defer backendsLock.Unlock()
	id, ok := addrToId[addr]
	if ok {
		return id
	}
	return -1
}

func getAddrFromId(id int) string {
	backendsLock.Lock()
	defer backendsLock.Unlock()
	addr, ok := idToAddr[id]
	if ok {
		return addr
	}
	return ""
}

func setBackend(id int, addr string) {
	backendsLock.Lock()
	defer backendsLock.Unlock()
	idToAddr[id] = addr
	addrToId[addr] = id
}

func getLeader() string {
	leaderLock.Lock()
	leader := leaderAddr
	leaderLock.Unlock()
	return leader
}

func setLeader(newLeaderId int) {
	backendsLock.Lock()
	defer backendsLock.Unlock()
	leaderLock.Lock()
	defer leaderLock.Unlock()
	leaderAddr = idToAddr[newLeaderId]
}

//dials to the server and sets up the readwrite stream
func serverConnInit(serverAddr string) (*ServerConnection) {
	conn, err := Open(serverAddr) //open the connection and get a stream
	if err != nil {
		log.Printf("Failed to open connection to %v: %v",serverAddr,err)
		return nil
	}
	rw := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))
	serverConn := ServerConnection{}
	serverConn.rw = rw
	serverConn.enc = gob.NewEncoder(rw)
	serverConn.dec = gob.NewDecoder(rw)
	serverConn.serverAddr = serverAddr
	serverConn.conn = conn
	return &serverConn
}

//keep a leaderConn global
//keep a leadrConn state (true is confirmed, false is not)
//have a pingack thread - every ping is declares not confirmed, every ack declares confirmed
//at the beginning ask for all ids 

func sendRequestToLeader(message *common.Message) *common.Message {
	var leaderConn *ServerConnection
	var res *common.Message
	for {
		leaderConn = serverConnInit(getLeader())
		if leaderConn == nil {
			log.Printf("Can't connect to leader\n")
			return nil
		}
		err := leaderConn.sendRequest(message)
		if err != nil {
			log.Printf("Error sending message: %#v\n", err)
			return nil
		}
		res = leaderConn.acceptRespnse()
		//TODO: handle case when response is not recieved (timeout)
		//TODO: handle failure of sendRequest
		for {
			if res.PrimaryType == "id" {
				log.Println("dropping extraneous id message...")
				res = leaderConn.acceptRespnse()
			if res.PrimaryType == "timeout" {
				return res
			}
			// this is the server telling us its id again
			} else {
				break
			}
		}

		if res.PrimaryType == "leaderId" {
			leaderId, _ := res.Request.(int)
			log.Printf("There's a new leader, changing to %s with id %d\n",getAddrFromId(leaderId), leaderId )
			setLeader(leaderId)
		} else {
			break
		}
	}
	if leaderConn != nil {
		(*leaderConn.conn).Close()
	}
	return res //at this point the message can only be ok or failure
	
}


func setUpBackendId(addr string) {
	for {
		serverConn := serverConnInit(addr)
		if serverConn != nil {
			serverConn.sendRequest(&common.Message{PrimaryType: "client", SecType: "Id"})
			res := serverConn.acceptRespnse()
			id, _ := res.Request.(int)
			log.Printf("Sever %s at id: %d \n", addr, id)
			setBackend(id, addr)
			(*serverConn.conn).Close()
			log.Printf("closed connection with %s\n", addr)
			if leaderAddr == "" {
				setLeader(id)
			}
			return
		} else {
			time.Sleep(time.Duration(500 * time.Millisecond))
		}
	}
}
func connManager() {
	//first get all server ids (keep a list of backends)
	
	
	log.Println("getting Ids..")
	leaderAddr = ""
	for _, addr := range(backends) {
		go setUpBackendId(addr)
	}
	
	//loop forever
		//take a message from the reqChan
		//send it to the leader
		// if getting back LeaderId -> change the leaderConn
		// if getting back failure - result is failure
		// if timeout, return to beginning of loop without taking a message out
}



//opens a connection to the backend and returns a stream
func Open(addr string) (*net.Conn, error) {
	log.Println("Dial " + addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%#v: Dialing "+addr+" failed", err)
	}
	return &conn, nil
}

//sends `request` to the backend server at serverAddr
//returns a stream to the caller (so it can fetch a response)
func (serverConn *ServerConnection) sendRequest(request *common.Message) (error) {
	
	log.Printf("Sending: \n%#v\n", request)
	
	err := serverConn.enc.Encode(request) //marshall the request
	if err != nil {
		return fmt.Errorf("%#v: Encode failed for struct: %#v", err, request)
	}
	err = serverConn.rw.Flush()
	if err != nil {
		return fmt.Errorf("%#v: Flush failed.",err)
	}
	log.Printf("Message Sent")
	return nil
}


//recieves and unmarshalls an id->Book map from the backend
func (serverConn *ServerConnection) acceptRespnse() *common.Message {
	var res *common.Message
	var err error
	timer := time.NewTimer(time.Duration( RESPONSETIMEOUT * time.Millisecond))
	decodeErr := make(chan error, 1)
	go func() {
		decodeErr <- serverConn.dec.Decode(&res)
	}()
	select {
		case err = <- decodeErr:
			if err != nil {
				log.Println("Error Decoding: ", err)
				return nil
			} else {
				return res
			}
		case <-timer.C:
			log.Println("timing out on message..")
			return &common.Message{PrimaryType: "timeout"}
	}
}


/*



//close the previous connection if it's given and dial to the back end
func (serverConn *ServerConnection) sendPing(conn net.Conn) (net.Conn, error) {
	if conn != nil {
		conn.Close()
	}
	newConn, err := net.Dial("tcp", serverConn.serverAddr)
	return newConn, err
}

//repeatedly ping the server
func (serverConn *ServerConnection) healthCheck() {
	conn, err := serverConn.sendPing(nil)
	timer := time.NewTimer(HEALTHINTERVAL * time.Second)
	for {
		if err != nil {
			log.Printf("Detected failure on %s at %#v\n", serverConn.serverAddr, time.Now().String())
		}
		<-timer.C
		conn, err = serverConn.sendPing(conn)
		timer = time.NewTimer(HEALTHINTERVAL * time.Second)
	}
}

//recieves and unmarshalls an id->Book map from the backend
func (serverConn *ServerConnection) recieveBookMap() (map[int]*Book, error) {
	var books map[int]*Book
	dec := gob.NewDecoder(serverConn.rw)
	err := dec.Decode(&books)
	fmt.Println(books)
	if err != nil {
		log.Println("Error Decoding: ", err)
		return nil, fmt.Errorf("%#v: Decoding Failed",err) 
	}
	log.Println("Response Recieved! ")
	return books, nil
}

//recieves and unmarshalls a single book from the backend
func (serverConn *ServerConnection) recieveBook() (Book, error) {
	var book Book
	dec := gob.NewDecoder(serverConn.rw)
	err := dec.Decode(&book)
	if err != nil {
		return Book{}, fmt.Errorf("%#v: Decoding Failed", err) 
	}
	log.Println("Response Recieved!")
	return book, nil
}


*/


//asks backend for all the books ans serves the index page
func getAllAndServeIndex(ctx iris.Context) {
	msg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "getall"}}
	res := sendRequestToLeader(msg)
	if res.PrimaryType != "ok" {
		ctx.View("error.html")
	} else {
		log.Printf("Got response: %#v\n", *res)
		allBooks := res.Request.(map[int32]*common.Book)
		serveIndex(ctx, allBooks)
	}
}

func getOneAndServerEdit(id int, ctx iris.Context) {
	msg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "getone", Book: common.Book{Id: int32(id)}}}
	res := sendRequestToLeader(msg)
	if res.PrimaryType != "ok" {
		ctx.View("error.html")
	} else {
		log.Printf("Got response: %#v\n", *res)
		book := res.Request.(common.Book)
		ctx.ViewData("Title", book.Title)
		ctx.ViewData("Author", book.Author)
		ctx.ViewData("Id", id)
		ctx.View("edit.html")
	}
}


//asks backend to add a book and redirects to index
func addBookAndServeIndex(ctx iris.Context){
    title, author := ctx.PostValue("title"), ctx.PostValue("author")
    msg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "new", Book: common.Book{Title: title, Author: author}}}
	res := sendRequestToLeader(msg)
	if res.PrimaryType != "ok" {
		ctx.View("error.html")
	} else {
		ctx.Redirect("/")
	}
}


//asks backend to delete a book and redirects to index
func deleteBookAndServeIndex(ctx iris.Context) {
	id, err := strconv.Atoi(ctx.PostValue("id"))
	if err != nil {
		log.Println("could not convert param to int: ", err)
	}
	msg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "delete", Book: common.Book{Id: int32(id)}}}
	res := sendRequestToLeader(msg)
	if res.PrimaryType != "ok" {
		ctx.View("error.html")
	} else {
		ctx.Redirect("/")
	}
}

//asks backend to change the fields of a specific book and redirects to index
func updateBookAndServeIndex(ctx iris.Context){
	id, _ := ctx.Params().GetInt("id")
	title, author := ctx.PostValue("title"), ctx.PostValue("author")
	msg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "update", Book: common.Book{Id: int32(id), Title: title, Author: author}}}
	res := sendRequestToLeader(msg)
	if res.PrimaryType != "ok" {
		ctx.View("error.html")
	} else {
		ctx.Redirect("/")
	}
}

//load the book data and serve the index page
func serveIndex(ctx iris.Context, books map[int32]*common.Book){
	ctx.ViewData("Books", books)
	ctx.View("index.html")
}


func parseCmdArgs(args []string) (string, []string){
	port := ":8080"
	var backendAddrs []string
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
					backendAddrs = append(backendAddrs, addr)
				} else {
					backendAddrs = append(backendAddrs, addr)
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
	return port, backendAddrs
}

func main() {
	gob.Register(common.RaftMessage{})
	gob.Register(common.ClientMessage{})
	gob.Register(map[int32]*common.Book{})
	gob.Register(common.Book{})
	idToAddr = make(map[int]string)
	addrToId = make(map[string]int)
	//fetch command line argument for custom port and backend address
	

	args := os.Args[1:]
	port, backendAddrs := parseCmdArgs(args)
	backends = backendAddrs
	
	//get server connection stream
	go connManager()
	time.Sleep(time.Duration(1 * time.Second))
	testMsg := &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "delete", Book: common.Book{Id: 1}}}
	res := sendRequestToLeader(testMsg)
	log.Printf("Got response: %#v\n", *res)

	testMsg = &common.Message{PrimaryType: "client", Request: common.ClientMessage{Cmd: "getall"}}
	res = sendRequestToLeader(testMsg)
	log.Printf("Got response: %#v\n", *res)
	allBooks := res.Request.(map[int32]*common.Book)
	log.Println(allBooks)
	

	
	app := iris.New()
	app.RegisterView(iris.HTML("./templates", ".html").Reload(true))

	//index page
	app.Get("/", func(ctx iris.Context) {
		getAllAndServeIndex(ctx)
	})

	//add page
	app.Get("/add", func(ctx iris.Context) {
		ctx.View("add.html")
	})

	//edit a specific record page
	app.Get("/edit/{id:int}", func(ctx iris.Context) {
		id, _ := ctx.Params().GetInt("id")
		getOneAndServerEdit(id, ctx)
	})

	//create a new book from new book page form
	app.Post("/new", func(ctx iris.Context) {
		addBookAndServeIndex(ctx)
	})

	//remove a book clicked on the index page "delete" button
	app.Post("/delete", func(ctx iris.Context) {
		deleteBookAndServeIndex(ctx)
	})

	//update the fields of a book based on form from the edit page
	app.Post("/update/{id:int}", func(ctx iris.Context) {
	    updateBookAndServeIndex(ctx)
	})

	//run the application and listen on determined port
	app.Run(iris.Addr(port), iris.WithoutServerError(iris.ErrServerClosed))
	
}