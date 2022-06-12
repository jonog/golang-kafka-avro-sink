package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro"
	"github.com/riferrei/srclient"

	kafka "github.com/segmentio/kafka-go"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

type Pokemon struct {
	Name string `avro:"name"`
}

// Kafka wire format
// https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func KafkaAvroMessage(schemaID int, inputMsg []byte) ([]byte, error) {
	var outputMsg []byte

	// Bytes 0
	// Magic Byte
	// Confluent serialization format version number; currently always 0.
	outputMsg = append(outputMsg, byte(0))

	// Bytes 1-4
	// Schema ID
	// 4-byte schema ID as returned by the Schema Registry
	binarySchemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaID, uint32(schemaID))
	outputMsg = append(outputMsg, binarySchemaID...)

	// Bytes 5-...
	// Data
	// Avro serialized data in Avro’s binary encoding.
	outputMsg = append(outputMsg, inputMsg...)
	return outputMsg, nil
}

func main() {
	kafkaURL := "0.0.0.0:9092"
	topic := "pokemon"

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schemaID := 1
	schema, err := schemaRegistryClient.GetSchema(schemaID)
	if err != nil {
		panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaID, err))
	}

	avroSchema, err := avro.Parse(schema.Schema())
	if err != nil {
		log.Fatal(err)
	}

	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("Start publishing")
	for i := 0; ; i++ {

		in := Pokemon{Name: "test" + fmt.Sprint(uuid.New())}

		data, err := avro.Marshal(avroSchema, in)
		if err != nil {
			log.Fatal(err)
		}

		binaryAvroMsg, err := KafkaAvroMessage(schemaID, data)
		if err != nil {
			log.Fatal(err)
		}

		key := fmt.Sprintf("Key-%d", i)

		msg := kafka.Message{
			Key:   []byte(key),
			Value: binaryAvroMsg,
			Headers: []kafka.Header{

				// ce_specversion: "1.0"
				// ce_type: "com.example.someevent"
				// ce_source: "/mycontext/subcontext"
				// ce_id: "1234-1234-1234"
				// ce_time: "2018-04-05T03:56:24Z"

				{Key: "ce_specversion", Value: []byte("1.0")},
				{Key: "ce_type", Value: []byte("com.example.user")},
			},
		}

		err = writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		time.Sleep(1 * time.Second)
	}
}
