package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	//reading filenames first to update the server
	files, err := ioutil.ReadDir("../../data/")
	if err != nil {
		log.Fatal(err)
	}

	toSend := ""
	for _, file := range files {
		toSend += " " + file.Name()
	}
	toSend = toSend[1:]

	fmt.Printf("message sent to server: %s", toSend)
	conn.Write([]byte(toSend))
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:1234")

	if err != nil {
		fmt.Printf("error encountered with connection setup")

	} else {
		handleConnection(conn)
	}
}
