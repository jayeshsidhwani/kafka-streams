package com.hotstar.streams;

import com.hotstar.utils.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;

import java.sql.Time;
import java.util.Properties;
import java.util.function.IntUnaryOperator;

/**
 * Created by jayeshsidhwani on 27/10/17.
 */
public class LocationLatencyStream {
    final private static String sourceTopic = "location-wise-stream-quality";
    final private static String akamaiSourceTopic = "akamai-benchmark-stream-quality";
    final private static String destinationTopic = "locations-with-low-qos";
    final private static String appId = "hotstar.demo.streams.location_qos-2";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        Properties config = StreamingConfig.GetConfig(bootstrapServers,
                appId,
                consumerId,
                Serdes.Integer().getClass().getName(),
                Serdes.Integer().getClass().getName(),
                5 * 1000);
        KStreamBuilder builder = getStreamProcessorTopology();

        KafkaStreams stream = new KafkaStreams(builder, config);
        stream.cleanUp();
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    private static KStreamBuilder getStreamProcessorTopology() {
        KStreamBuilder builder = new KStreamBuilder();

        final KStream<Integer, Integer> input = builder.stream(sourceTopic);
        final KTable<Integer, Integer> akamaiLow = builder.table(akamaiSourceTopic);

        KStream<Windowed<Integer>, Integer> twoSecondLow = input
                .groupByKey()
                .reduce((v1, v2) -> v1 > v2 ? v1 : v2, TimeWindows.of(2 * 1000L), "min-quality")
                .toStream();

        KStream<Integer, Integer> inferiorStream  = twoSecondLow
                .selectKey((k, v) -> k.key())
                .join(akamaiLow,
                        (value1, value2) -> {
                            if (value1 < value2) {
                                return value1;
                            } else {
                                return null;
                            }
                        });
        inferiorStream
                .filterNot((k, v) -> v == null)
                .to(destinationTopic);

        return builder;
    }

    static Integer check(Integer left, Integer right) {
        return left;
    }
}
