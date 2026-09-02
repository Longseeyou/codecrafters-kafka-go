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
	buf := make([]byte, 4)
	// binary.BigEndian.PutUint32(buf, 4)
	buf = []byte{0, 0, 0, 4}
	conn.Write(buf)
	if _, err := conn.Write(message[8:12]); err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return

	}
	buf = make([]byte, 2)
	buf = []byte{2, 3}
	if _, err := conn.Write(buf); err != nil {
		fmt.Println("error writing to connection: ", err.Error())
		return
	}
}
