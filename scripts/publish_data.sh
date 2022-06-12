#!/bin/bash

# use Kafka REST Proxy API V2 to initiate topic and schema registry set up

curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
      -H "Accept: application/vnd.kafka.v2+json" \
      --data '{"value_schema": "{\"type\": \"record\", \"name\": \"Pokemon\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "Pikachu"}},{"value": {"name": "Eevee"}},{"value": {"name": "Snorlax"}},{"value": {"name": "Garchomp"}}]}' \
      "http://localhost:8082/topics/pokemon"
