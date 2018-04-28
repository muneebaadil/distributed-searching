package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var heartbeatFreq int

func handleConnection(conn net.Conn, dataDir string, chunkIds string) {
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
		time.Sleep(time.Duration(heartbeatFreq) * time.Second)
		conn.Write([]byte("heartbeat"))
		log.Printf("heartbeat sent")
	}
}

func main() {
	serverAddress := flag.String("serverAddress", "127.0.0.1:3000", "IP and port of server")
	dataDir := flag.String("dataDir", "../../data/chunks", "data folder containing all chunks")
	chunkIds_ := flag.String("chunkIds", "1 2 3", "identifiers of chunk a slave is hosting")
	flag.IntVar(&heartbeatFreq, "heartbeatFreq", 2, "time (in seconds) after which to send"+
		" periodic heartbeat")
	flag.Parse()
	//TOFIX: CLI functionality for chunkIds

	conn, err := net.Dial("tcp", *serverAddress)

	if err != nil {
		fmt.Printf("error encountered with connection setup")

	} else {
		handleConnection(conn, *dataDir, *chunkIds_)
	}
}
