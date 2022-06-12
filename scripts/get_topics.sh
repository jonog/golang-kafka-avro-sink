#!/bin/bash

curl -X GET -H "Accept: application/vnd.kafka.v2+json" \
      "http://localhost:8082/topics"
