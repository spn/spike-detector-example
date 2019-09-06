package com.example.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonMapDeserializer implements Deserializer<Map<String, Object>> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<Map<String, Object>> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonMapDeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<Map<String, Object>>) props.get("JsonPOJOClass");
    }

    @Override
    public Map<String, Object> deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        Map<String, Object> data;
        try {
            data = objectMapper.readValue(bytes, Map.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}
