package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type RequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
}

type ResponseHeader struct {
	CorrelationID uint32
}

type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

type APIVersionsResponse struct {
	ErrorCode uint16
	APIKeys   []APIKey
}

func encodeAPIVersionsResponse(header ResponseHeader, body APIVersionsResponse) []byte {
	payload := make([]byte, 0)
	payload = binary.BigEndian.AppendUint32(payload, header.CorrelationID) // correlation_id
	payload = binary.BigEndian.AppendUint16(payload, body.ErrorCode)       // error_code
	payload = append(payload, byte(len(body.APIKeys)+1))                   // api_keys array length
	for _, key := range body.APIKeys {
		payload = binary.BigEndian.AppendUint16(payload, key.APIKey)     // api_key
		payload = binary.BigEndian.AppendUint16(payload, key.MinVersion) // api_key min_version
		payload = binary.BigEndian.AppendUint16(payload, key.MaxVersion) // api_key max_version
	}
	payload = append(payload, byte(0))                  // TAG_BUFFER
	payload = binary.BigEndian.AppendUint32(payload, 0) // throttle_time_ms
	payload = append(payload, byte(0))                  // TAG_BUFFER

	// Construct the returned frame now that we know size
	frame := make([]byte, 0, len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	return frame
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
