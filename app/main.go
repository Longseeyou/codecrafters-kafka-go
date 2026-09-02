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

	message := make([]byte, 1024)
	if _, err := conn.Read(message); err != nil {
		fmt.Println("error reading from connection: ", err.Error())
		return
	}
	response := make([]byte, 23)

	// There are 19 bytes after the 4-byte size field.
	binary.BigEndian.PutUint32(response[0:4], 19)

	// Response header
	copy(response[4:8], message[8:12]) // correlation ID

	// Response body
	binary.BigEndian.PutUint16(response[8:10], 0) // error code

	response[10] = 2 // compact array: 1 entry + 1

	// API key entry
	binary.BigEndian.PutUint16(response[11:13], 18) // API key
	binary.BigEndian.PutUint16(response[13:15], 0)  // minimum version
	binary.BigEndian.PutUint16(response[15:17], 4)  // maximum version

	response[17] = 0 // API-key entry tagged fields

	binary.BigEndian.PutUint32(response[18:22], 0) // throttle time
	response[22] = 0                               // response tagged fields

	if _, err := conn.Write(response); err != nil {
		fmt.Println("error writing response:", err)
		return
	}
}
