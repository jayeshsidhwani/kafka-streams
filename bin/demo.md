## Running Demos

### Running PopularSearch example
- Run the stream processor: `java -cp target/examples-1.0-SNAPSHOT-standalone.jar com.hotstar.streams.PopularSearchTermsStream`
- Open popularity topic: `./bin/kafka-console-consumer --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --topic popular-search-terms-output.temp --from-beginning`
- Run search producer: `java -cp target/examples-1.0-SNAPSHOT-standalone.jar com.hotstar.producers.SearchTermProducer`




### Running LocationQOS example
- Start the stream: `java -cp target/examples-1.0-SNAPSHOT-standalone.jar com.hotstar.streams.LocationLatencyStream`
- Run the consumer `./bin/kafka-console-consumer --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer --property key.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer --topic locations-with-low-qos --from-beginning`
- Run the akamai producer `java -cp target/examples-1.0-SNAPSHOT-standalone.jar com.hotstar.producers.AkamaiLocationBenchmarkProducer`
- Run the location reporter `java -cp target/examples-1.0-SNAPSHOT-standalone.jar com.hotstar.producers.LocationWiseStreamQualityProducer`



