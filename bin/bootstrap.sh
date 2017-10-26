#!/bin/bash

# Create a topic
cd $HOME/Developer/kafka/platform;
./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sum-of-odd-numbers-input;
./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sum-of-odd-numbers-output;

./bin/kafka-console-producer --broker-list localhost:9092 --topic sum-of-odd-numbers-input \
    --property value.serializer=org.apache.kafka.common.serialization.IntegerSerializer \
    --property "parse.key=true" \
    --property "key.separator=:"
