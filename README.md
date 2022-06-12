# Golang Kafka Avro Sink POC

## Background
POC to demonstrate a Golang application for consuming Avro encoded messages from Kafka and writing Avro OCF files, without needing to decode the binary message payload.

This enables a generic solution for consuming from Kafka and writing to local file or cloud object storage, without implementing any topic/schema specific code.

For POC purposes, message decoding logic has been added for validation/testing.

## Stack
* Kafka
* Schema Registry
* Kafka REST Proxy
* Script to bootstrap topic/schema using REST Proxy (`scripts/publish_data.sh`)
* Golang Producer (`producer/producer.go`)
* Golang Consumer (`consumer/consumer.go`)
* Golang Avro OCF Validation script (`decoder/decode.go`)

## Getting started
Open several terminals.

*Terminal #1:* Run Kafka stack
```
docker-compose up
```

*Terminal #2:* Setup topic, schema and run consumer
```
./scripts/publish_data.sh
./scripts/get_topics.sh (to validate topic has been created)
go mod download
go run consumer/consumer.go
```

*Terminal #3:* Publish 
```
go run producer/producer.go (& kill after ~10 seconds)
```
Check for presence of `*.avro` files in directory:
```
ls *.avro
```
Pick a file, `filename.avro` and validate it can be decoded successfully.
```
go run decode/decode.go filename.avro
```

## Consumer logic
* Connect to Kafka
* Read message from Kafka
* Validate message can be decoded/parsed (for validation/testing purposes)
* Parse schema ID from Kafka message payload & retrieve schema from schema registry
* Read Kafka headers (e.g. exract for use within cloud object storage)
* Construct OCF using schema and extracted bytes
* Write Avro OCF to local file
* Explicitly commit offset

## Golang Kafka libraries selected
* Kafka library: [github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go)
* Schema registry client: [github.com/riferrei/srclient](https://github.com/riferrei/srclient)
* Avro decoder/encoder: [github.com/hamba/avro](https://github.com/hamba/avro)
See `go.mod` for specific versions.

## More info on Binary Formats

*Kafka Wire Format:*
* https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format

*Avro OCF Format:*
* https://avro.apache.org/docs/current/spec.html#Object+Container+Files
