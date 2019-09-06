package com.example.streams;

import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;

public class StandardDeviationAggregator implements Aggregator<String, Map<String, Object>, StandardDeviationValue> {

    @Override
    public StandardDeviationValue apply(String key, Map<String, Object> value, StandardDeviationValue aggregate) {
        Number rate = value == null ? null : (Number) value.get("rate");
        return new StandardDeviationValue(aggregate, rate == null ? 0 : rate.longValue());
    }
}
