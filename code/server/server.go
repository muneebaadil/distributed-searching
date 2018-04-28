package main

import (
	"encoding/gob"
	"flag"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

//some structures for better information management
type slave struct {
	id         int
	load       int
	connection net.Conn
}

func (s *slave) sendQuery(msg message) {
	log.Printf("message sending: type %s, client %d, slave %d, chunk %d\n", msg.messageType,
		msg.clientID, msg.slaveID, msg.chunkID)

	enc := gob.NewEncoder(s.connection)
	enc.Encode(&msg)
	s.load++
}

// func (s *slave) recvQuery() message {
// 	return message{}
// }
//
type client struct {
	id      int
	channel chan message
}

type message struct {
	messageType string
	clientID    int
	slaveID     int
	chunkID     int
}

//global maps for book-keeping
var chunks = make(map[int][]int)    //chunk ID -> slave IDs
var slaves = make(map[int]*slave)   //slave ID -> pointer to slave object
var clients = make(map[int]*client) //client ID -> pointer to client object
var nextSlave = 1                   //ID of next joining slave
var nextClient = 1                  //ID of next joining client

//misc
var bufferSize = 4096
var numChunks int
var timeout int

//a few auxliary functions specific to message structure
//and the implementation
func _connRead(conn net.Conn, buffer []byte) string {
	n, err := conn.Read(buffer)
	if err != nil {
		return "timeout"
	} else {
		return string(buffer[:n])
	}
}

func _connSendMsg(conn net.Conn, msg message) {

}

func _connRecvMsg(conn net.Conn) message {
	return message{}
}

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
	thisSlave := slave{id: slaveID, load: 0, connection: conn}
	slaves[slaveID] = &thisSlave

	//retrieving the record of data chunks the new slave has
	buffer := make([]byte, bufferSize)
	n, _ := conn.Read(buffer)
	newMsg := string(buffer[:n])
	chunkIds := strings.Split(newMsg, " ")

	//updating chunk directory
	for _, chunkId_ := range chunkIds {
		chunkId, _ := strconv.Atoi(chunkId_)
		chunks[chunkId] = append(chunks[chunkId], slaveID)
	}

	//indefinite reading of responses from slave
	for true {
		conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		n, err := conn.Read(buffer)

		if err != nil {
			conn.Close()
			log.Printf("connection lost")

			//re-reoute pending requests to other slaves
			break
		} else {
			newMsg = string(buffer[:n])
		}
	}
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
	clients[clientID] = &newClient

	buffer := make([]byte, bufferSize)
	n, _ := conn.Read(buffer)
	toFind := string(buffer[:n])
	log.Printf("client %d: string to find: %s", clientID, toFind)

	scheduleJobs(toFind, clientID)
}

func scheduleJobs(toFind string, myID int) {

	//iterating over all chunks to search on
	for i := 1; i <= numChunks; i++ {
		slaveIds, ok := chunks[i]

		if ok == false {
			log.Printf("chunk %d isn't connected to any slave", i)

		} else {
			minLoad := math.MaxInt32
			minLoadSlaveID := -1

			//finding minimum load of all slaves currently hosting chunks[i]
			for _, currSlaveID := range slaveIds {
				currSlave := slaves[currSlaveID]

				if currSlave.load < minLoad {
					minLoad = currSlave.load
					minLoadSlaveID = currSlaveID
				}
			}

			//sending search request to slave having minimum load
			msg := message{messageType: "S", clientID: myID, slaveID: minLoadSlaveID, chunkID: i}
			slaves[minLoadSlaveID].sendQuery(msg)

			log.Printf("client %d: searching in chunk %d through slave %d (load=%d)",
				myID, i, minLoadSlaveID, slaves[minLoadSlaveID].load-1)
		}
	}
}

func main() {

	//command line parsing
	portSlaves := flag.String("portSlaves", "3000", "port number for slaves connection")
	portClients := flag.String("portClients", "3001", "port number for clients connection")
	flag.IntVar(&numChunks, "numChunks", 4, "total number of chunks (must be"+
		" numbered from 1-numChunks inclusive)")
	flag.IntVar(&timeout, "timeout", 3, "time threshold (in seconds) for heartbeat from slaves")
	flag.Parse()

	//simalteneously listening for clients and slaves
	go listenForSlaves(*portSlaves)
	listenForClients(*portClients)
}
