package com.hotstar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class LocationWiseStreamQualityProducer {

    final private static String sourceTopic = "location-wise-stream-quality";
    final private static String appId = "hotstar.demo.streams.location_quality";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Producer<Integer, Integer> producer = getProducer(bootstrapServers);

        final Integer[] locationIds = new Integer[]{1, 2, 3, 4, 5, 6};
        Random randomStreamPicker = new Random();

        for (Integer i = 0; i < Integer.MAX_VALUE; i++) {
            for (Integer l = 0; l < locationIds.length; l++) {
                Integer randomStream = randomStreamPicker.nextInt(720) + 80;

                final ProducerRecord<Integer, Integer> record = new ProducerRecord<Integer, Integer>(sourceTopic, l, randomStream);
                producer.send(record).get();

                System.out.printf("Location %d reported a stream quality of %d\n", record.key(), record.value());
            }
            TimeUnit.MILLISECONDS.sleep(400);
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
