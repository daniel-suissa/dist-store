package main

import (
	"./infra"
	"./raft"
	"./business_logic"
	"os"
	"sync"
	"fmt"
	"strings"

)

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
	business_logic.InitBookMap()
	raft := raft.InitRaft()
	infra.StartInfra(port, backendAddrs, id, raft.MsgQueue)
	//go messageThread()
	wg.Wait()
}