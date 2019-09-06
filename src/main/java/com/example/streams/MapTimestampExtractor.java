package com.example.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Map;

public class MapTimestampExtractor implements TimestampExtractor {

    private final String fieldName;

    public MapTimestampExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Map<String, Object> data = (Map<String, Object>) record.value();
        Long ts = (Long) data.get(fieldName);
        return ts == null ? -1 : ts;
    }
}
