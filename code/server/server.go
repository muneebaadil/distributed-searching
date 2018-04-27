package main

import (
	"flag"
	"log"
	"net"
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
	newSlave := slave{id: slaveID, load: 0, channel: make(chan message)}
	slaves[slaveID] = newSlave

	//retrieving the record of data chunks the new slave has
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
		go runClients(conn, nextClient)

		nextClient++
	}
}

func runClients(conn net.Conn, clientID int) {
	//registering new client
	newClient := client{id: clientID, channel: make(chan message)}
	clients[clientID] = newClient

	//retrieving the query information..
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

// func handleSlaves(conn net.Conn) {
// 	//first reading list of chunks from the newly arrived slave
// 	buffer := make([]byte, 4096)
// 	n, _ := conn.Read(buffer)
// 	newMsg := string(buffer[:n])
// 	fileNames := strings.Split(newMsg, " ")

// 	//updating filesLog
// 	for _, fileName := range fileNames {
// 		conns, ok := filesLog[fileName]

// 		fmt.Printf("adding %s file\n", fileName)

// 		if ok == false { //if no such file is held by any slave
// 			toAdd := []net.Conn{conn}
// 			filesLog[fileName] = toAdd

// 		} else { //if the file is already held by some some slave(s)
// 			conns = append(conns, conn)
// 			filesLog[fileName] = conns
// 		}
// 	}
// }

// func listenForSlaves(portNum string) {
// 	ln, err := net.Listen("tcp", ":"+portNum)
// 	log.Printf("Listening for slaves on %s", portNum)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	for true {
// 		conn, err := ln.Accept()
// 		if err != nil {
// 			log.Fatal(err)
// 			continue
// 		}

// 		go handleSlaves(conn)
// 	}
// }

// func handleClients(conn net.Conn) {

// }

// func listenForClients(portNum string) {
// 	ln, err := net.Listen("tcp", ":"+portNum)
// 	log.Printf("Listening for clients on %s", portNum)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	for true {
// 		conn, err := ln.Accept()
// 		if err != nil {
// 			log.Fatal(err)
// 			continue
// 		}

// 		go handleClients(conn)
// 	}
// }
