package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

// key value pair of (filename, list of connections)
var filesLog = make(map[string][]net.Conn)

func handleSlaves(conn net.Conn) {
	//first reading list of chunks from the newly arrived slave
	buffer := make([]byte, 4096)
	n, _ := conn.Read(buffer)
	newMsg := string(buffer[:n])
	fileNames := strings.Split(newMsg, " ")

	//updating filesLog
	for _, fileName := range fileNames {
		conns, ok := filesLog[fileName]

		fmt.Printf("adding %s file\n", fileName)

		if ok == false { //if no such file is held by any slave
			toAdd := []net.Conn{conn}
			filesLog[fileName] = toAdd

		} else { //if the file is already held by some some slave(s)
			conns = append(conns, conn)
			filesLog[fileName] = conns
		}
	}
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

		go handleSlaves(conn)
	}
}

func handleClients(conn net.Conn) {

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

		go handleClients(conn)
	}
}

func main() {

	go listenForSlaves("1234")
	listenForClients("1235")
}
