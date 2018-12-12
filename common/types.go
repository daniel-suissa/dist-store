package common

import (
	"net"
)
type Message struct {
	Tid int32
	PrimaryType string
	SecType string
	NodeAddr string
	NodeId int
	Request interface{}
}

type RaftMessage struct {
	Term int
	Data interface{}
	LastComittedLog int
}

type AppendMessage struct {
	Term int
	LogNum int
	Msg ClientMessage
	IsCommited bool
}

type CommitMessage struct {
	Start int
	End int
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