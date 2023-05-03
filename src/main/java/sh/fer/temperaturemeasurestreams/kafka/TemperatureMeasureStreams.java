package sh.fer.temperaturemeasurestreams.kafka;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import sh.fer.temperaturemeasurestreams.domain.SensorMeasurements;

public class TemperatureMeasureStreams {

    private final Serde<String> stringSerde = Serdes.String();

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<SensorMeasurements> sensorMeasurementsSerde = new ObjectMapperSerde<>(SensorMeasurements.class);

        KStream<String, SensorMeasurements> stream = builder.stream(
                "sensor-data", Consumed.with(stringSerde, sensorMeasurementsSerde)
        );

        stream.foreach((key, value) -> System.out.println("New data received: " + value));

        stream.to("sensor-data");

        return builder.build();
    }

}
