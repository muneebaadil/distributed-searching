package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

//some structures for better information management
type slave struct {
	id      int
	load    int
	channel chan message
}

type client struct {
	id      int
	channel chan message
}

type message struct {
	messageType string
	clientID    int
	slaveID     int
}

//global maps for book-keeping
var chunks = make(map[int][]int)   //chunk ID -> slave IDs
var slaves = make(map[int]slave)   //slave ID -> slave object
var clients = make(map[int]client) //client ID -> client object
var nextSlave = 1                  //ID of next joining slave
var nextClient = 1                 //ID of next joining client

//misc
var bufferSize = 4096

func listenForSlaves(portNum string) {

	ln, err := net.Listen("tcp", ":"+portNum)
	log.Printf("Listening for slaves on %s", portNum)
	if err != nil {
		log.Fatal(err)
	}

	for true {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
			continue
		}

		log.Printf("Slave %d Connected!", nextSlave)
		go runSlave(conn, nextSlave)

		nextSlave++
	}
}

func runSlave(conn net.Conn, slaveID int) {
	//registering new slave
	thisSlave := slave{id: slaveID, load: 0, channel: make(chan message)}
	slaves[slaveID] = thisSlave

	//retrieving the record of data chunks the new slave has
	buffer := make([]byte, bufferSize)
	n, _ := conn.Read(buffer)
	newMsg := string(buffer[:n])
	fmt.Printf("new message: %s\n", newMsg)
	chunkIds := strings.Split(newMsg, " ")

	//updating chunk directory
	for _, chunkId_ := range chunkIds {
		chunkId, _ := strconv.Atoi(chunkId_)
		chunks[chunkId] = append(chunks[chunkId], slaveID)
	}

	//code here for functionality
}

func listenForClients(portNum string) {
	ln, err := net.Listen("tcp", ":"+portNum)
	log.Printf("Listening for clients on %s", portNum)

	if err != nil {
		log.Fatal(err)
	}

	for true {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
			continue
		}

		log.Printf("Client %d Connected!", nextClient)
		go runClient(conn, nextClient)

		nextClient++
	}
}

func runClient(conn net.Conn, clientID int) {
	//registering new client
	newClient := client{id: clientID, channel: make(chan message)}
	clients[clientID] = newClient

	buffer := make([]byte, bufferSize)
	n, _ := conn.Read(buffer)
	toFind := string(buffer[:n])
	log.Printf("Client %d: String to find: %s", clientID, toFind)

	for true {
	}
}

func main() {

	//command line parsing
	portSlaves := flag.String("portSlaves", "3000", "port number for slaves connection")
	portClients := flag.String("portClients", "3001", "port number for clients connection")
	flag.Parse()

	//simalteneously listening for clients and slaves
	go listenForSlaves(*portSlaves)
	listenForClients(*portClients)
}
