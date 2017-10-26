package com.hotstar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by jayeshsidhwani on 26/10/17.
 */
public class OddSumProducer {
    final private static String sourceTopic = "sum-of-odd-numbers-input";
    final private static String appId = "hotstar.demo.streams.window_sum";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Producer<Integer, Integer> producer = getProducer(bootstrapServers);

        for (Integer i = 0; i < Integer.MAX_VALUE; i++) {
            final ProducerRecord<Integer, Integer> record =
                    new ProducerRecord<Integer, Integer>(sourceTopic, i, i + 1);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset());

            TimeUnit.MILLISECONDS.sleep(10);
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
