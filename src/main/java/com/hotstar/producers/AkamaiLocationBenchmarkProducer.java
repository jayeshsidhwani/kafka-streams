package com.hotstar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AkamaiLocationBenchmarkProducer {

    final private static String sourceTopic = "akamai-benchmark-stream-quality";
    final private static String appId = "hotstar.demo.streams.akamai_benchmark_stream";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Producer<Integer, Integer> producer = getProducer(bootstrapServers);

        final HashMap<Integer, Integer> locationBenchmarks = new HashMap<Integer, Integer>();
        locationBenchmarks.put(1, 720);
        locationBenchmarks.put(2, 640);
        locationBenchmarks.put(3, 880);
        locationBenchmarks.put(4, 420);
        locationBenchmarks.put(5, 620);
        locationBenchmarks.put(6, 120);

        final HashMap<Integer, Integer> newLocationBenchmarks = new HashMap<Integer, Integer>();
        newLocationBenchmarks.put(1, 230);
        newLocationBenchmarks.put(2, 840);
        newLocationBenchmarks.put(3, 180);
        newLocationBenchmarks.put(4, 68);
        newLocationBenchmarks.put(5, 920);
        newLocationBenchmarks.put(6, 520);

        for (Integer i = 0; i < Integer.MAX_VALUE; i++) {
            for (Map.Entry<Integer, Integer> entry : locationBenchmarks.entrySet()) {
                Integer key = entry.getKey();
                Integer value = entry.getValue();

                final ProducerRecord<Integer, Integer> record = new ProducerRecord<Integer, Integer>(sourceTopic, key, value);
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("Location %s reported a benchmark of %s\n",
                        record.key(), record.value());
            }

            TimeUnit.SECONDS.sleep(7);

            for (Map.Entry<Integer, Integer> entry : newLocationBenchmarks.entrySet()) {
                Integer key = entry.getKey();
                Integer value = entry.getValue();

                final ProducerRecord<Integer, Integer> record = new ProducerRecord<Integer, Integer>(sourceTopic, key, value);
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("Location %s reported a benchmark of %s\n",
                        record.key(), record.value());
            }
        }
        TimeUnit.SECONDS.sleep(7);

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
