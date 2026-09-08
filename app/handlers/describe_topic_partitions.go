package handlers

import (
	"encoding/binary"
	"fmt"
)

type Topic struct {
	ErrorCode            uint16
	TopicName            string
	TopicID              [16]byte
	IsInternal           bool
	Partitions           []Partition
	AuthorizedOperations int32
}

type Partition struct {
	// Partition fields can be added here as needed
}

type DescribeTopicPartitionsRequest struct {
	Topics []Topic
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []Topic
	NextCursor     int8
}

func EncodeDescribeTopicPartitionsResponse(header ResponseHeader, body DescribeTopicPartitionsResponse) []byte {
	payload := make([]byte, 0)
	payload = binary.BigEndian.AppendUint32(payload, header.CorrelationID) // correlation_id
	payload = append(payload, byte(0))                                     // TAG_BUFFER
	payload = binary.BigEndian.AppendUint32(payload, 0)                    // throttle_time_ms
	fmt.Println("number of topics: ", len(body.Topics))
	payload = append(payload, byte(len(body.Topics)+1)) // topics array length
	for _, topic := range body.Topics {
		payload = binary.BigEndian.AppendUint16(payload, 3)   // error_code (UNKNOWN_TOPIC_OR_PARTITION)
		payload = append(payload, byte(len(topic.TopicName))) // topic_name length
		payload = append(payload, []byte(topic.TopicName)...) // topic_name
		payload = append(payload, topic.TopicID[:]...)        // topic_id
		payload = append(payload, byte(0))                    // is_internal
		payload = append(payload, byte(1))                    // partitions array
		payload = binary.BigEndian.AppendUint32(payload, 0)   // authorized_operations
		payload = append(payload, byte(0))                    // TAG_BUFFER
	}
	payload = append(payload, byte(0xff)) // next_cursor (-1)
	payload = append(payload, byte(0))    // TAG_BUFFER

	frame := make([]byte, 0, len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	return frame
}

func DecodeDescribeTopicPartitionsRequest(payload []byte) DescribeTopicPartitionsRequest {
	request := DescribeTopicPartitionsRequest{}
	currentIndex := 8 // Start after the API request header (8 bytes)
	clientIDLength := binary.BigEndian.Uint16(payload[currentIndex : currentIndex+2])
	currentIndex += 2 + int(clientIDLength)       // Move index past client_id
	currentIndex++                                // skip TAG_BUFFER
	arrayLength := int(payload[currentIndex]) - 1 // topics array length
	for i := 0; i < arrayLength; i++ {
		// Decode each topic name (simplified, actual implementation may vary)
		topicNameLength := int(payload[currentIndex]) - 1 // topic name length
		currentIndex++
		topicName := string(payload[currentIndex : currentIndex+topicNameLength])
		currentIndex += topicNameLength
		topic := Topic{
			TopicName: topicName,
		}
		request.Topics = append(request.Topics, topic)
		currentIndex++ // Skip TAG_BUFFER for each topic
	}
	return request
}
