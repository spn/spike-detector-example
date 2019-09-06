package com.example.streams;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class ErrorSpikeDetector {
    public static void main(String[] args) {

        OptionParser parser = new OptionParser();
        OptionSpec<String> serverOption = parser.accepts("server", "Kafka bootstrap servers").withOptionalArg().ofType(String.class).defaultsTo("localhost:9092");
        OptionSpec<String> topicOption = parser.accepts("topic", "Source Kafka topic").withRequiredArg().ofType(String.class);
        OptionSet options = parser.parse(args);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "spike-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverOption.value(options));
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);       // dev only!
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);            // dev only!
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<Map<String, Object>> jsonSerde = Serdes.serdeFrom(new JsonMapSerializer(), new JsonMapDeserializer());
        final Serde<StandardDeviationValue> stdDevSerde = Serdes.serdeFrom(StandardDeviationValue.SERIALIZER, StandardDeviationValue.DESERIALIZER);

        builder.stream(topicOption.value(options),
                Consumed.with(Serdes.String(), jsonSerde, new MapTimestampExtractor("timestamp"), Topology.AutoOffsetReset.LATEST))
                .groupBy((key, value) -> key)
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((KeyValueMapper<Windowed<String>, Long, KeyValue<String, Map<String, Object>>>) (key, value) -> {
                    final Map<String, Object> result = new HashMap<>();
                    result.put("timestamp", key.window().start());
                    result.put("user_id", Long.parseLong(key.key()));
                    result.put("rate", value);
                    return pair(key.key(), result);
                })
                .to("error_rates", Produced.with(Serdes.String(), jsonSerde));

        final KStream<String, Map<String, Object>> errorRateStream = builder.
                stream("error_rates",
                        Consumed.with(Serdes.String(), jsonSerde,
                                new MapTimestampExtractor("timestamp"), Topology.AutoOffsetReset.LATEST));

        final KTable<String, StandardDeviationValue> sigmaTable = errorRateStream
                .groupBy((key, value) -> key)
                .windowedBy(TimeWindows.of(Duration.ofMinutes(15)).grace(Duration.ofSeconds(5)))
                .aggregate(() -> new StandardDeviationValue(0),
                        new StandardDeviationAggregator(),
                        Materialized.with(Serdes.String(), stdDevSerde))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .groupBy((key, value) -> KeyValue.pair(key.key(), value),
                        Grouped.with(Serdes.String(), stdDevSerde))
                .aggregate(() -> new StandardDeviationValue(0), new Aggregator<String, StandardDeviationValue, StandardDeviationValue>() {
                    @Override
                    public StandardDeviationValue apply(String key, StandardDeviationValue value, StandardDeviationValue aggregate) {
                        return value;
                    }
                }, (key, value, aggregate) -> value, Materialized.with(Serdes.String(), stdDevSerde));

        errorRateStream.leftJoin(sigmaTable, (value1, value2) -> {
            final Map<String, Object> result = new HashMap<>(value1);
            if (value2 == null) {
                value2 = new StandardDeviationValue(0);
            }
            result.put("std_dev", value2.compute());
            result.put("average", value2.average());
            result.put("sample_count", value2.count());
            return result;
        }, Joined.with(Serdes.String(), jsonSerde, stdDevSerde))
                .filter((key, value) -> {
                    Double stdDev = (Double) value.get("std_dev");
                    Double average = (Double) value.get("average");
                    Integer count = (Integer) value.get("sample_count");
                    if (count == null || count < 1) {
                        // don't know sigma value at the moment, skip.
                        return false;
                    }
                    // a rate comes out as an integer, not long because Jackson does it for some reason.
                    Integer rate = (Integer) value.get("rate");
                    return (rate > average + 3 * stdDev);
                }).print(Printed.<String, Map<String, Object>>toSysOut().withLabel("SPIKE"));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());
        streams.setStateListener((newState, oldState) -> System.out.println("Streams state: " + newState));
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
