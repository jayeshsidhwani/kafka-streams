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
public class PopularSearchTermsStream {

    final private static String sourceTopic = "search-input";
    final private static String destinationTopic = "popular-search-terms-output.temp";
    final private static String appId = "hotstar.demo.streams.popular_search";
    final private static String consumerId = appId + ".client-1";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        Properties config = StreamingConfig.GetConfig(bootstrapServers,
                appId,
                consumerId,
                Serdes.String().getClass().getName(),
                Serdes.String().getClass().getName());
        KStreamBuilder builder = getStreamProcessorTopology();

        KafkaStreams stream = new KafkaStreams(builder, config);
        stream.cleanUp();
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    // UserId:SearchTerm
    // Return popular SearchTerm in last 2 seconds
    private static KStreamBuilder getStreamProcessorTopology() {
        KStreamBuilder builder = new KStreamBuilder();

        final KStream<Integer, String> input = builder.stream(Serdes.Integer(), Serdes.String(), sourceTopic);

        input
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(TimeWindows.of(5 * 1000L), "popular-search-terms")
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value))
                .to(Serdes.String(), Serdes.Long(), destinationTopic);

        return builder;
    }

}
