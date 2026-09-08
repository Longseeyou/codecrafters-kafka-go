package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

func receive(r io.Reader) (payload []byte, error error) {
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeBytes); err != nil {
		return nil, err
	}

	messageSize := binary.BigEndian.Uint32(sizeBytes)
	payload = make([]byte, messageSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		payload, err := receive(conn)
		if err != nil {
			fmt.Println("Error receiving request: ", err.Error())
			return
		}
		apiRequestHeader := handlers.DecodeAPIversionRequestHeader(payload)
		switch apiRequestHeader.APIKey {
		case 18: // API Versions
			handleAPIVersionsRequest(conn, payload)
		case 75: // DescribeTopicPartitions
			handleDescribeTopicPartitionsRequest(conn, payload)
		}
	}
}

func handleAPIVersionsRequest(conn net.Conn, payload []byte) {
	apiRequestHeader := handlers.DecodeAPIversionRequestHeader(payload)
	responseHeader := handlers.ResponseHeader{
		CorrelationID: apiRequestHeader.CorrelationID,
	}
	errorCode := handlers.CheckAPIVersion(apiRequestHeader.APIKey, apiRequestHeader.APIVersion)
	responseBody := handlers.APIVersionsResponse{
		ErrorCode: errorCode,
		APIKeys: []handlers.APIKey{
			{APIKey: 18, MinVersion: 0, MaxVersion: 4},
			{APIKey: 75, MinVersion: 0, MaxVersion: 0},
		},
	}
	response := handlers.EncodeAPIVersionsResponse(responseHeader, responseBody)
	if _, err := conn.Write(response); err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func handleDescribeTopicPartitionsRequest(conn net.Conn, payload []byte) {
	apiRequestHeader := handlers.DecodeAPIversionRequestHeader(payload)
	responseHeader := handlers.ResponseHeader{
		CorrelationID: apiRequestHeader.CorrelationID,
	}
	// errorCode := handlers.CheckAPIVersion(apiRequestHeader.APIKey, apiRequestHeader.APIVersion)
	describeTopicPartitionsRequest := handlers.DecodeDescribeTopicPartitionsRequest(payload)
	responseBody := handlers.DescribeTopicPartitionsResponse{
		Topics: describeTopicPartitionsRequest.Topics,
	}
	response := handlers.EncodeDescribeTopicPartitionsResponse(responseHeader, responseBody)

	if _, err := conn.Write(response); err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

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
