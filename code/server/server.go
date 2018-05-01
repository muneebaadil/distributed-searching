package main

import (
	"flag"
	"fmt"
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
	msgStr := msg2str(msg)
	//log.Printf("message sent: %s", msgStr)
	s.connection.Write([]byte(msgStr))
	s.load++
}

func (s *slave) sendHalt(msg message) {
	msgStr := msg2str(msg)
	s.connection.Write([]byte(msgStr))
	//s.load--
	//log.Printf("(h) %d load decremented to %d\n", s.id, s.load)
}

func (s *slave) routeResp2Client(msgStr string) {
	newMsg := str2msg(msgStr)
	// log.Printf("(d) msg type %s, tofind %s, slave %d, client %d, chunk %d \n",
	// 	newMsg.messageType, newMsg.toFind, newMsg.slaveID, newMsg.clientID, newMsg.chunkID)
	clients[newMsg.clientID].channel <- newMsg
	s.load--
}

type client struct {
	id      int
	channel chan message
}

type message struct {
	messageType string
	clientID    int
	slaveID     int
	chunkID     int
	toFind      string
}

//tuple of two integers
type intint struct {
	first  int
	second int
}

func msg2str(msg message) string {
	out := fmt.Sprintf("%s %d %d %d %s ", msg.messageType, msg.slaveID,
		msg.clientID, msg.chunkID, msg.toFind)
	return out
}

func str2msg(str string) message {
	temp := strings.Split(str, " ")
	msgType := temp[0]
	slaveID, _ := strconv.Atoi(temp[1])
	clientID, _ := strconv.Atoi(temp[2])
	chunkID, _ := strconv.Atoi(temp[3])
	toFind := temp[4]
	out := message{messageType: msgType, slaveID: slaveID,
		clientID: clientID, chunkID: chunkID, toFind: toFind}
	return out
}

func remove(s []int, r int) []int {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
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
		n, err := conn.Read(buffer)

		if err != nil {
			conn.Close()
			log.Printf("Slave %d: Connection lost\n", slaveID)

			// book-keeping removal
			delete(slaves, slaveID)
			for chunkID, slaveIds := range chunks {
				if len(slaveIds) == 1 {
					delete(chunks, chunkID)
				} else {
					chunks[chunkID] = remove(slaveIds, slaveID)
				}
			}

			// send signal to all clients so they could re-route their
			// packets which aren't returned
			for _, v := range clients {
				errorMsg := message{messageType: "E", slaveID: slaveID}
				v.channel <- errorMsg
			}

		} else {
			newMsgStr := string(buffer[:n])
			//fmt.Printf("new incoming message: %s\n", newMsgStr)
			if newMsgStr != "heartbeat" { //if a processing message
				slaves[slaveID].routeResp2Client(newMsgStr)
			}
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

	reqSlaveIds := scheduleJobs(toFind, clientID)
	isFound := false
	numResps := 0

	for numResps < len(reqSlaveIds) {
		newMsg := <-clients[clientID].channel

		if newMsg.messageType == "N" || newMsg.messageType == "T" {
			numResps++

			log.Printf("client %d (num responses = %d): message %s, slave %d, chunk %d",
				clientID, numResps, newMsg.messageType, newMsg.slaveID, newMsg.chunkID)

		} else if newMsg.messageType == "F" {
			numResps++
			isFound = true
			conn.Write([]byte("1"))

			log.Printf("client %d (num responses = %d): message %s, slave %d, chunk %d",
				clientID, numResps, newMsg.messageType, newMsg.slaveID, newMsg.chunkID)

			foundRespID := intint{newMsg.clientID, newMsg.chunkID}
			haltMsg := message{messageType: "H", slaveID: reqSlaveIds[0],
				clientID: clientID, chunkID: 1}

			broadcastHalt(haltMsg, reqSlaveIds, foundRespID, clientID)

		} else if newMsg.messageType == "E" {
			if isFound == true {
				numResps++

			} else {
				//re-routing packets which were previously planned for failed slav
				//YOU HAVE FAILED THIS PACKET, SLAVE!!
				// for chunkID, reqSlaveId := range reqSlaveIds {
				// 	if reqSlaveId == newMsg.slaveID {

				// 	}
				// }
			}
		}
	}

	if isFound == false {
		conn.Write([]byte("0"))
	}

	//book-keeping removal
	delete(clients, clientID)
}

func broadcastHalt(haltMsg message, reqSlaveIds []int, foundRespID intint, clientID int) {

	for chunkID, slaveID := range reqSlaveIds {

		haltMsg.slaveID = slaveID
		haltMsg.chunkID = chunkID + 1

		if (foundRespID != intint{clientID, chunkID + 1}) {
			log.Printf("client %d: sending halt message to slave %d, chunk %d\n",
				clientID, slaveID, chunkID+1)
			slaves[slaveID].sendHalt(haltMsg)
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
}

func scheduleChunk(i int, toFind string, myID int) int {
	slaveIds, ok := chunks[i]
	minLoadSlaveID := -1

	if ok == false {
		log.Printf("chunk %d isn't connected to any slave", i)

	} else {
		minLoad := math.MaxInt32

		//finding minimum load of all slaves currently hosting chunks[i]
		for _, currSlaveID := range slaveIds {
			currSlave, ok := slaves[currSlaveID]

			if ok == true {
				if currSlave.load < minLoad {
					minLoad = currSlave.load
					minLoadSlaveID = currSlaveID
				}
			}
		}

		//sending search request to slave having minimum load
		msg := message{messageType: "S", clientID: myID, slaveID: minLoadSlaveID,
			chunkID: i, toFind: toFind}
		log.Printf("client %d: searching in chunk %d through slave %d (load=%d)",
			myID, i, minLoadSlaveID, slaves[minLoadSlaveID].load)
		slaves[minLoadSlaveID].sendQuery(msg)

		time.Sleep(time.Duration(1) * time.Second)
	}

	return minLoadSlaveID
}

func scheduleJobs(toFind string, myID int) []int {
	reqSlaveIds := []int{}
	//iterating over all chunks to search on
	for i := 1; i <= numChunks; i++ {
		ans := scheduleChunk(i, toFind, myID)
		if ans != -1 {
			reqSlaveIds = append(reqSlaveIds, ans)
		}
	}
	return reqSlaveIds
}

func main() {

	//command line parsing
	portSlaves := flag.String("portSlaves", "3000", "port number for slaves"+
		" connection")
	portClients := flag.String("portClients", "3001", "port number for"+
		" clients connection")
	flag.IntVar(&numChunks, "numChunks", 4, "total number of chunks (must be"+
		" numbered from 1-numChunks inclusive)")
	flag.IntVar(&timeout, "timeout", 3, "time threshold (in seconds) for"+
		" heartbeat from slaves")
	flag.Parse()

	//simalteneously listening for clients and slaves
	go listenForSlaves(*portSlaves)
	listenForClients(*portClients)
}
