package ru.yandex.practicum.telemetry.collector.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.kafka.CollectorKafkaProperties;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler{

    private final EventProducer producer;
    private final CollectorKafkaProperties kafkaProperties;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        ScenarioAddedEventProto added = event.getScenarioAdded();

        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(added.getName())
                .setConditions(added.getConditionList().stream().map(this::mapCondition).toList())
                // Для repeated-поля Protobuf генерирует метод getXxxList()
                .setActions(added.getActionList().stream().map(this::mapAction).toList())
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

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto condition) {
        return ScenarioConditionAvro.newBuilder()
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(mapConditionValue(condition))
                .build();
    }

    private Object mapConditionValue(ScenarioConditionProto condition) {
        return switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();   // пришло true/false → кладём Boolean
            case INT_VALUE -> condition.getIntValue();     // пришло число → кладём Integer
            case VALUE_NOT_SET -> null;                    // ничего не пришло → null
        };
    }

    private DeviceActionAvro mapAction(DeviceActionProto action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setValue(action.hasValue() ? action.getValue() : null) // union { null, int } value = null;
                .build();
    }
}
