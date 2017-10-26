package com.hotstar.streams;

import com.hotstar.utils.StreamingConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Created by jayeshsidhwani on 26/10/17.
 */
public class OddSumStream {
    final private static String sourceTopic = "sum-of-odd-numbers-input";
    final private static String destinationTopic = "sum-of-odd-numbers-output";
    final private static String appId = "hotstar.demo.streams.sum";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        Properties config = StreamingConfig.GetConfig(bootstrapServers,
                appId,
                consumerId,
                Serdes.Integer().getClass().getName(),
                Serdes.Integer().getClass().getName());
        KStreamBuilder builder = getStreamProcessorTopology();

        KafkaStreams stream = new KafkaStreams(builder, config);
        stream.cleanUp();
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    private static KStreamBuilder getStreamProcessorTopology() {
        KStreamBuilder builder = new KStreamBuilder();

        final KStream<Integer, Integer> input = builder.stream(sourceTopic);
        final KTable<Integer, Integer> sumOfOddNumbers = input
                .filter((k, v) -> v % 2 != 0)
                // We want to compute the total sum across ALL numbers, so we must re-key all records to the
                // same key.  This re-keying is required because in Kafka Streams a data record is always a
                // key-value pair, and KStream aggregations such as `reduce` operate on a per-key basis.
                // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
                // all records.
                .selectKey((k, v) -> 1)
                .groupByKey()
                .reduce((v1, v2) -> (v1 + v2), "sum");

        sumOfOddNumbers.to(Serdes.Integer(), Serdes.Integer(), destinationTopic);


        return builder;
    }
}
