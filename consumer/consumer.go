package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hamba/avro"
	"github.com/hamba/avro/ocf"
	"github.com/riferrei/srclient"
	kafka "github.com/segmentio/kafka-go"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type Pokemon struct {
	Name string `avro:"name"`
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func writeOCF(filename string, schema string, data []byte) error {

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	// note: defaults to null compression (not deflate)
	enc, err := ocf.NewEncoder(schema, f)
	if err != nil {
		log.Fatal(err)
	}

	// write avro data into OCF without validation
	_, err = enc.Write(data)
	if err != nil {
		log.Fatal(err)
	}

	if err := enc.Flush(); err != nil {
		log.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}
	return err
}

func main() {
	kafkaURL := "0.0.0.0:9092"
	topic := "pokemon"
	groupID := "test-group-id-1"

	reader := getKafkaReader(kafkaURL, topic, groupID)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	ctx := context.Background()
	for {

		// TODO: read messages and control offset commit
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		fmt.Println(m.Time.String())

		for i, s := range m.Headers {
			fmt.Println(i, s.Key, string(s.Value))
		}

		schemaID := binary.BigEndian.Uint32(m.Value[1:5])
		fmt.Println(schemaID)

		// note schema lookup is cached locally
		schema, err := schemaRegistryClient.GetSchema(int(schemaID))
		if err != nil {
			panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaID, err))
		}

		fmt.Println("SCHEMA:")
		fmt.Println(schema.Schema())

		// temp: validate check message can be decoded

		schemaAvro, err := avro.Parse(schema.Schema())
		if err != nil {
			log.Fatal(err)
		}
		out := Pokemon{}
		err = avro.Unmarshal(schemaAvro, m.Value[5:], &out)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Avro parsed message...")
		fmt.Println(out)

		// write .avro file in OCF format

		filename := "test_" + RandStringRunes(4) + ".avro"
		writeOCF(filename, schema.Schema(), m.Value[5:])

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}

	}
}
