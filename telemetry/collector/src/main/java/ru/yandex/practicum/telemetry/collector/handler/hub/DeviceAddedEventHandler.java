package ru.yandex.practicum.telemetry.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.kafka.CollectorKafkaProperties;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler{

    private final EventProducer producer;
    private final CollectorKafkaProperties kafkaProperties;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        DeviceAddedEventProto added = event.getDeviceAdded();

        DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                .setId(added.getId())
                .setType(DeviceTypeAvro.valueOf(added.getType().name()))
                .build();
        Instant timestamp = Instant.ofEpochSecond(                 // Protobuf Timestamp → Instant
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos());

        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        producer.send(kafkaProperties.topics().hubs(), event.getHubId(), timestamp, avro);
    }
}
