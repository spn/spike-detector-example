# spike-detector-example

This is a Kafka Streams based code, providing a sample topology to compute standard deviation over an aggregated data. The value of standard deviation is then used on a joined (enriched) stream to detect potential spikes.

This topology is sub-optimal and is only provided as a proof of concept.


## Building

Use `./gradlew build` to build a runnable JAR file.

## Running

```java -jar build/spike-detector-1.0.0.jar --server KAFKA:9092 --topic SOURCE_TOPIC```

If you see glibc-related issues (MacOS or Windows), try docker run:

```docker run --rm -it -v $(PWD)/build/libs:/home:Z openjdk /usr/bin/java -jar /home/spike-detector-1.0.0.jar --server KAFKA:9092 --topic SOURCE_TOPIC```

(your Docker container may need a special setup to pair with local Kafka)
