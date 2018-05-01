package main

import (
	"flag"
	"log"
	"net"
)

func main() {
	serverAddress := flag.String("serverAddress", "127.0.0.1:3001", "IP and port "+
		"of server")
	toFind := flag.String("toFind", "helloworld", "string to search from the server")
	bufferSize := flag.Int("bufferSize", 10, "buffer size of buffer to store "+
		"incoming messages in")
	flag.Parse()

	conn, err := net.Dial("tcp", *serverAddress)
	if err != nil {
		log.Printf("Error encountered while connecting to server at %s", *serverAddress)
	} else {
		//sending string to search
		conn.Write([]byte(*toFind))

		//recieving the server's response
		buffer := make([]byte, *bufferSize)
		n, _ := conn.Read(buffer)

		if string(buffer[:n]) == "1" {
			log.Printf("Text found")
		} else if string(buffer[:n]) == "0" {
			log.Printf("Text NOT found")
		} else {
			log.Printf("INVALID RESPONSE")
		}
	}
}
