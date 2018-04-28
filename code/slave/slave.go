package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

//some structures for better information management
type request struct {
	msg     message
	channel chan message
}

type message struct {
	messageType string
	clientID    int
	slaveID     int
	chunkID     int
}

//tuple of two integers
type intint struct {
	first  int
	second int
}

//global variable(s) for bookkeeping
var requests = make(map[intint]request)

//misc
var heartbeatFreq int
var dataDir string
var chunkIds_ string

//helper function for filereading
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func handleConnection(conn net.Conn, chunkIds string) {
	//first updating the server about the chunks this slave has
	conn.Write([]byte(chunkIds))

	//parsing the chunks_ID string into list for convenience
	chunkIdsStr := strings.Split(chunkIds, " ")
	chunkIds_ := []int{}
	for _, chunkIdStr := range chunkIdsStr {
		temp, _ := strconv.Atoi(chunkIdStr)
		chunkIds_ = append(chunkIds_, temp)
	}

	//functionality code here
	for true {
		dec := gob.NewDecoder(conn)
		newMsg := &message{}
		dec.Decode(newMsg)

		log.Printf("new message: type %s, client %d, slave %d, chunk %d", newMsg.messageType,
			newMsg.clientID, newMsg.slaveID, newMsg.chunkID)

		reqID := intint{newMsg.clientID, newMsg.chunkID}
		req, ok := requests[reqID]

		if ok == false { //if new request
			//registering a new request
			newReq := request{msg: *newMsg, channel: make(chan message)}
			requests[reqID] = newReq

			//delegating the request to go routine
			go handleRequest(conn, reqID)

		} else { //if request already being handled
			req.channel <- *newMsg
		}
	}
}

func handleRequest(conn net.Conn, reqID intint) {
	fileName := fmt.Sprintf("%s/%d.txt", dataDir, requests[reqID].msg.chunkID)
	f, err := os.Open(fileName)

	if err != nil {
		log.Printf("error opening file %s", fileName)
		return

	} else {
		r := bufio.NewReader(f)
		s, err := Readln(r)
		for err == nil {
			fmt.Printf("pass: %s", s)
			break
		}
	}
}

func sendHeartbeats(conn net.Conn) {
	for true {
		time.Sleep(time.Duration(heartbeatFreq) * time.Second)
		conn.Write([]byte("heartbeat"))
		//log.Printf("heartbeat sent")
	}
}
func main() {
	serverAddress := flag.String("serverAddress", "127.0.0.1:3000", "IP and port of server")
	flag.StringVar(&dataDir, "dataDir", "../../data/chunks", "data folder containing all chunks")
	flag.StringVar(&chunkIds_, "chunkIds", "1 2 3", "identifiers of chunk a slave is hosting")
	flag.IntVar(&heartbeatFreq, "heartbeatFreq", 2, "time (in seconds) after which to send"+
		" periodic heartbeat")
	flag.Parse()
	//TOFIX: CLI functionality for chunkIds

	conn, err := net.Dial("tcp", *serverAddress)

	if err != nil {
		fmt.Printf("error encountered with connection setup")

	} else {
		go sendHeartbeats(conn)
		handleConnection(conn, chunkIds_)
	}
}
