package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		fmt.Println("accepted")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	if _, err := conn.Read(buf); err != nil {
		fmt.Println("error reading from connection: ", err.Error())
		return
	}
	messageSize := binary.BigEndian.Uint32(buf[0:4])
	apiKey := binary.BigEndian.Uint16(buf[4:6])
	apiVersion := binary.BigEndian.Uint16(buf[6:8])
	correlationID := binary.BigEndian.Uint32(buf[8:12])
	fmt.Printf("Message Size: %d, API Key: %d, API Version: %d, Correlation ID: %d\n", messageSize, apiKey, apiVersion, correlationID)
	if _, err := conn.Write(buf[8:12]); err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}
}
