package com.hotstar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by jayeshsidhwani on 26/10/17.
 */
public class SearchTermProducer {
    final private static String sourceTopic = "search-input";
    final private static String destinationTopic = "popular-search-terms-output.temp";
    final private static String appId = "hotstar.demo.streams.popular_search";
    final private static String consumerId = appId + ".client-1";
    final private static String[] searchTerms = new String[]{
            "sarabhai-vs-sarabhai",
            "ipl                 ",
            "mumbai-indians      ",
            "chennai-super-kings ",
            "sachin tendulkar"
    };

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        final Producer<Integer, String> producer = getProducer(bootstrapServers);
        Random rand = new Random();

        for (Integer i = 0; i < Integer.MAX_VALUE; i++) {
            Integer randomIndex = rand.nextInt(5);

            final ProducerRecord<Integer, String> record =
                    new ProducerRecord<Integer, String>(sourceTopic, i, searchTerms[randomIndex]);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Someone searched %s\n", record.value());

            TimeUnit.MILLISECONDS.sleep(200);
        }
    }

    private static Producer<Integer, String> getProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, consumerId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
