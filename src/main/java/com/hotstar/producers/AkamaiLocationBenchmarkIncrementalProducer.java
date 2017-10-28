package com.hotstar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class AkamaiLocationBenchmarkIncrementalProducer {

    final private static String sourceTopic = "akamai-benchmark-stream-quality";
    final private static String appId = "hotstar.demo.streams.akamai_benchmark_stream";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        Scanner input = new Scanner(System.in);
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Producer<Integer, Integer> producer = getProducer(bootstrapServers);


        for (Integer i = 0; i < Integer.MAX_VALUE; i++) {
            String quality = input.nextLine();
            String[] locationStream = quality.split(":");

            final ProducerRecord<Integer, Integer> record = new ProducerRecord<Integer, Integer>(sourceTopic, Integer.parseInt(locationStream[0]), Integer.parseInt(locationStream[1]));
            RecordMetadata metadata = producer.send(record).get();

            System.out.printf("sent record(key=%s value=%s)\n",
                    record.key(), record.value());
        }

    }

    private static Producer<Integer, Integer> getProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, consumerId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
