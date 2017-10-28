package com.hotstar.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * Created by jayeshsidhwani on 26/10/17.
 */
public class StreamingConfig {

    final private static String StateDirConfig = "/tmp/kafka-com.hotstar.streams";
    final private static String AutoOffsetResetConfig = "earliest";
    final private static Integer CommitIntervalMS = 2 * 1000;


    public static Properties GetConfig(String bootstrapServers,
                                       String appId,
                                       String consumerId,
                                       String keySerdesKlass,
                                       String valueSerdesKlass) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, consumerId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetConfig);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdesKlass);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdesKlass);
        props.put(StreamsConfig.STATE_DIR_CONFIG, StateDirConfig);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, CommitIntervalMS);

        return props;
    }
}
