package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

const clusterMetadataLogFilePathPattern = "/tmp/kraft-combined-logs/%s-%s/00000000000000000000.log"

func loadClusterMetadata(path string) (handlers.ClusterMetadata, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return handlers.ClusterMetadata{}, err
	}

	return handlers.DecodeClusterMetadataRequest(payload)
}

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

func handleConnection(conn net.Conn, metadata *handlers.ClusterMetadata) {
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
			handleDescribeTopicPartitionsRequest(conn, payload, metadata)
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

func extractClusterMetadata(metadata *handlers.ClusterMetadata) handlers.TopicMetadata {
	topicMetadata := handlers.TopicMetadata{
		Topics:     make(map[string]handlers.Topic),
		Partitions: make(map[handlers.UUID][]handlers.Partition),
	}
	for _, batch := range metadata.RecordBatches {
		for _, record := range batch.Records {
			switch value := record.Value.(type) {
			case handlers.PartitionRecord:
				topicMetadata.Partitions[value.TopicUUID] = append(
					topicMetadata.Partitions[value.TopicUUID],
					handlers.Partition{
						ErrorCode:              0,
						PartitionIndex:         value.PartitionID,
						LeaderID:               value.Leader,
						LeaderEpoch:            value.LeaderEpoch,
						ReplicaNodes:           value.InSyncReplicas,
						ISRNodes:               value.InSyncReplicas,
						EligibleLeaderReplicas: make([]int32, 0),
						LastKnownELR:           make([]int32, 0),
						OfflineReplicas:        make([]int32, 0),
					},
				)
			case handlers.TopicRecord:
				topicMetadata.Topics[value.TopicName] = handlers.Topic{
					TopicName: value.TopicName,
					TopicID:   value.TopicUUID,
				}
			}
		}
	}

	for name, topic := range topicMetadata.Topics {
		topic.Partitions = topicMetadata.Partitions[topic.TopicID]
		topicMetadata.Topics[name] = topic
	}
	return topicMetadata
}

func handleDescribeTopicPartitionsRequest(conn net.Conn, payload []byte, metadata *handlers.ClusterMetadata) {
	apiRequestHeader := handlers.DecodeAPIversionRequestHeader(payload)
	responseHeader := handlers.ResponseHeader{
		CorrelationID: apiRequestHeader.CorrelationID,
	}
	// errorCode := handlers.CheckAPIVersion(apiRequestHeader.APIKey, apiRequestHeader.APIVersion)

	topicMetadata := extractClusterMetadata(metadata)

	describeTopicPartitionsRequest := handlers.DecodeDescribeTopicPartitionsRequest(payload, topicMetadata)
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

	metadata, err := loadClusterMetadata(
		"/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
	)
	if err != nil {
		fmt.Println("Failed to load cluster metadata:", err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		fmt.Println("accepted")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, &metadata)
	}
}
