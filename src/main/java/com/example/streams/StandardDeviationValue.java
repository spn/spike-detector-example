package com.example.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;

public class StandardDeviationValue {

    private final int size;
    private final long[] buckets;

    public StandardDeviationValue(int size) {
        this.size = size;
        this.buckets = new long[size];
    }

    public StandardDeviationValue(StandardDeviationValue source, long rate) {
        this.size = source.size + 1;
        this.buckets = new long[this.size];
        System.arraycopy(source.buckets, 0, this.buckets, 0, source.buckets.length);
        this.buckets[this.size - 1] = rate;
    }

    public int count() {
        return size;
    }

    public double average() {
        if (size == 0) {
            return 0.0;
        }
        double average = 0.0;
        for(long rate : buckets) {
            average += rate;
        }
        average /= size;
        return average;
    }

    public double compute() {
        if (size == 0) {
            return 0.0;
        }
        final double average = average();
        double deviation = 0.0;
        for(long rate : buckets) {
            double dev = rate - average;
            deviation += dev * dev;
        }
        deviation /= size;
        return Math.sqrt(deviation);
    }

    private static final IntegerSerializer INT_SERIALIZER = new IntegerSerializer();

    public static final Serializer<StandardDeviationValue> SERIALIZER = new Serializer<StandardDeviationValue>() {
        @Override
        public byte[] serialize(String topic, StandardDeviationValue data) {
            if (data == null) {
                return null;
            }

            byte[] buf = new byte[4 + 8 * data.buckets.length];
            System.arraycopy(INT_SERIALIZER.serialize(topic, data.buckets.length), 0, buf, 0, 4);
            int pos = 4;
            for(long rate : data.buckets) {
                buf[pos++] = (byte) (rate >>> 56);
                buf[pos++] = (byte) (rate >>> 48);
                buf[pos++] = (byte) (rate >>> 40);
                buf[pos++] = (byte) (rate >>> 32);
                buf[pos++] = (byte) (rate >>> 24);
                buf[pos++] = (byte) (rate >>> 16);
                buf[pos++] = (byte) (rate >>> 8);
                buf[pos++] = (byte) rate;
            }
            return buf;
        }
    };

    public static final Deserializer<StandardDeviationValue> DESERIALIZER = new Deserializer<StandardDeviationValue>() {
        @Override
        public StandardDeviationValue deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            int size = 0;
            for(int i = 0; i < 4; i++) {
                size <<= 8;
                size |= data[i] & 0xFF;
            }

            StandardDeviationValue value = new StandardDeviationValue(size);
            for(int i = 0; i < size; i++) {
                long rate = 0;
                for(int j = 0; j < 8; j++) {
                    rate <<= 8;
                    rate |= data[4 + 8 * i + j] & 0xFF;
                }
                value.buckets[i] = rate;
            }
            return value;
        }
    };
}

