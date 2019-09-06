package com.example.streams;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.HashMap;
import java.util.Map;

@UdafDescription(name = "std_dev", description = "Standard deviation")
public class StdDeviationFunction {

    @UdafFactory(description = "computes standard deviation using buckets of values")
    // KSQL only support TWO arguments. We assume bucket size is fixed to 1 minute;
    // std_dev(timestamp_column, window_size_seconds, bucket_size_seconds)
    public static Udaf<Long, Map<String, Double>> createStdDeviationAggregation(final long bucketsInWindow) {
        final long windowSizeMillis = bucketsInWindow * 60000;    // 1 min
        return new Udaf<Long, Map<String, Double>>() {

            private long[] buckets;
            private long start = -1;

            @Override
            public Map<String, Double> initialize() {
                Map<String, Double> result = new HashMap<>();
                result.put("count", 0.0);
                result.put("mean", 0.0);
                result.put("sum_sqr", 0.0);
                return result;
            }

            @Override
            public Map<String, Double> aggregate(final Long timestamp, Map<String, Double> data) {
                long start = (timestamp / windowSizeMillis) * windowSizeMillis;
                if (this.start == -1) {
                    this.start = start;
                } else if (this.start != start) {
                    throw new IllegalStateException("Expected window start at " + this.start + " but got " + start + ", window size: " + windowSizeMillis + ", timestsamp: " + timestamp);
                }
                int bucket = (int) ((timestamp - start) / 60000);
                buckets[bucket]++;
                return null;
            }

            private double average() {
                double sum = 0.0;
                for(long rate : buckets) {
                    sum += rate;
                }
                return sum / buckets.length;
            }

            private double stddev(double avg) {
                double sum = 0.0;
                for(long rate : buckets) {
                    sum += (rate - avg) * (rate - avg);
                }
                return Math.sqrt(sum / buckets.length);
            }

            @Override
            public Map<String, Double> merge(Map<String, Double> stringDoubleMap, Map<String, Double> a1) {
                return null;
            }
        };
    }

}
