package main

import (
	"bufio"
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

//global variable(s) for bookkeeping
var requests = make(map[intint]*request)

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
	buffer := make([]byte, 4096)

	for true {
		n, _ := conn.Read(buffer)
		newMsg := str2msg(string(buffer[:n]))

		reqID := intint{newMsg.clientID, newMsg.chunkID}
		req, ok := requests[reqID]

		if ok == false { //if new request

			if newMsg.messageType != "H" {
				//registering a new request
				newReq := &request{msg: newMsg, channel: make(chan message)}
				requests[reqID] = newReq
				log.Printf("Req (client = %d, chunk = %d): Registered!\n", reqID.first,
					reqID.second)

				//delegating the request to go routine
				go handleRequest(conn, reqID, newReq, newMsg.toFind)
			}

		} else { //if request already being handled
			req.channel <- newMsg
		}
	}
}

func handleRequest(conn net.Conn, reqID intint, req *request, toFind string) {
	fileName := fmt.Sprintf("%s/%d.txt", dataDir, requests[reqID].msg.chunkID)
	//fmt.Printf("reading file %s, to find = %s\n", fileName, toFind)
	f, err := os.Open(fileName)

	if err != nil {
		log.Printf("error opening file %s", fileName)
		return

	} else {
		r := bufio.NewReader(f)
		isFound := false
		isHalt := false
		s, err := Readln(r)

		for (err == nil) && (isFound == false) && (isHalt == false) {
			select {
			case <-req.channel:
				isHalt = true
				log.Printf("Req (Client = %d, Chunk = %d): TERMINATED!\n", reqID.first,
					reqID.second)
			default:
				if s == toFind {
					isFound = true
					log.Printf("Req (Client = %d, Chunk = %d): FOUND!\n", reqID.first,
						reqID.second)
				}
				s, err = Readln(r)
			}
		}

		//deleting the request ASAP, as the whole processing is done to generate a
		// response
		delete(requests, reqID)
		outMsg := req.msg

		if isHalt == true {
			outMsg.messageType = "T"
		} else if isFound == true {
			outMsg.messageType = "F"
		} else {
			outMsg.messageType = "N"
			log.Printf("Req (Client = %d, Chunk = %d): NOT FOUND\n", reqID.first,
				reqID.second)
		}

		conn.Write([]byte(msg2str(outMsg)))
	}
}

func sendHeartbeats(conn net.Conn) {
	for true {
		time.Sleep(time.Duration(heartbeatFreq) * time.Second)
		//conn.Write([]byte("heartbeat"))
	}
}
func main() {
	serverAddress := flag.String("serverAddress", "127.0.0.1:3000", "IP and port "+
		"of server")
	flag.StringVar(&dataDir, "dataDir", "../../data/chunks", "data folder "+
		"containing all chunks")
	flag.StringVar(&chunkIds_, "chunkIds", "1 2 3", "identifiers of chunk a slave "+
		"is hosting")
	flag.IntVar(&heartbeatFreq, "heartbeatFreq", 2, "time (in seconds) after which "+
		"to send periodic heartbeat")
	flag.Parse()

	conn, err := net.Dial("tcp", *serverAddress)

	if err != nil {
		fmt.Printf("error encountered with connection setup")

	} else {
		go sendHeartbeats(conn)
		handleConnection(conn, chunkIds_)
	}
}
