package com.example.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonMapSerializer implements Serializer<Map<String, Object>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonMapSerializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Map<String, Object> data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
