package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/handlers"
)

type topic struct {
	topic_name string
	topic_id   [16]byte
}

type DescribeTopicPartitionsResponse struct {
	topics []topic
}

func encodeDescribeTopicPartitionsResponse(header ResponseHeader, body DescribeTopicPartitionsResponse) []byte {
	payload := make([]byte, 0)
	payload = binary.BigEndian.AppendUint32(payload, header.CorrelationID) // correlation_id
	payload = append(payload, byte(0))                                     // TAG_BUFFER
	payload = binary.BigEndian.AppendUint32(payload, 0)                    // throttle_time_ms
	payload = append(payload, byte(len(body.topics)+1))                    // topics array length
	payload = binary.BigEndian.AppendUint16(payload, 3)                    // error_code (UNKNOWN_TOPIC_OR_PARTITION)
	for _, topic := range body.topics {
	}
}

func receive(r io.Reader) (RequestHeader, error) {
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeBytes); err != nil {
		return RequestHeader{}, err
	}

	messageSize := binary.BigEndian.Uint32(sizeBytes)
	payload := make([]byte, messageSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return RequestHeader{}, err
	}

	return decodeRequestHeader(payload), nil
}

func decodeRequestHeader(payload []byte) RequestHeader {
	request := RequestHeader{}
	request.APIKey = binary.BigEndian.Uint16(payload[0:2])
	request.APIVersion = binary.BigEndian.Uint16(payload[2:4])
	request.CorrelationID = binary.BigEndian.Uint32(payload[4:8])
	return request
}

func CheckAPIVersion(apiKey uint16, apiVersion uint16) uint16 {
	if apiKey == 18 {
		if apiVersion >= 0 && apiVersion <= 4 {
			return 0 // No error
		} else {
			return 35 // Unsupported version
		}
	}
	return 0
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		request, err := receive(conn)
		if err != nil {
			fmt.Println("Error receiving request: ", err.Error())
			return
		}

		apiKey := APIKey{
			APIKey:     18,
			MinVersion: 0,
			MaxVersion: 4,
		}

		DescribeTopicPartitions := APIKey{
			APIKey:     75,
			MinVersion: 0,
			MaxVersion: 0,
		}

		responseHeader := ResponseHeader{CorrelationID: request.CorrelationID}
		responseBody := APIVersionsResponse{
			ErrorCode: 0,
			APIKeys:   []APIKey{apiKey, DescribeTopicPartitions},
		}
		responseBody.ErrorCode = CheckAPIVersion(request.APIKey, request.APIVersion)

		response := encodeAPIVersionsResponse(responseHeader, responseBody)
		if _, err := conn.Write(response); err != nil {
			fmt.Println("Error sending response: ", err.Error())
			return
		}
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
