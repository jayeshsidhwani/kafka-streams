package com.hotstar.streams;

import com.hotstar.utils.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Created by jayeshsidhwani on 27/10/17.
 */
public class LocationLatencyStream {
    final private static String sourceTopic = "location-wise-latency";
    final private static String akamaiSourceTopic = "akamai-location-wise-latency";
    final private static String destinationTopic = "location-latency-summary";
    final private static String appId = "hotstar.demo.streams.location_latency";
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
        final KStream<Integer, Integer> akamaiLow = builder.stream(akamaiSourceTopic);

        final KTable<Integer, Integer> incomingStreamRates = input
                .groupByKey()
                .reduce((v1, v2) -> v1 > v2 ? v2 : v1);

        final KTable<> lowerThanBenchmark = incomingStreamRates.join(akamaiLow,
                (streamRate, benchmarkStreamRate) -> {
                    if (streamRate < benchmarkStreamRate)
                        return
                }

        )
//        final KTable<Windowed<String>, AvgValue> locationAverage = input
//                .groupByKey()
//                .<String, AvgValue>aggregate(
//                        () -> new AvgAggregator<String, Integer, AvgValue>(),
//                        TimeWindows.of(10000),
//                        Serdes.String(), new AvgValueSerializer());
//

        return builder;
    }
}
