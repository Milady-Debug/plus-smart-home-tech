package ru.yandex.practicum.telemetry.collector.handler.sensor;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.CollectorKafkaProperties;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ClimateSensorEventHandler implements SensorEventHandler {

    private final EventProducer producer;
    private final CollectorKafkaProperties kafkaProperties;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        ClimateSensorProto climate = event.getClimateSensor();     // данные именно климат-датчика

        ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                .setTemperatureC(climate.getTemperatureC())
                .setHumidity(climate.getHumidity())
                .setCo2Level(climate.getCo2Level())
                .build();

        Instant timestamp = Instant.ofEpochSecond(                 // Protobuf Timestamp → Instant
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos());

        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        producer.send(kafkaProperties.topics().sensors(), event.getHubId(), timestamp, avro);
    }
}
