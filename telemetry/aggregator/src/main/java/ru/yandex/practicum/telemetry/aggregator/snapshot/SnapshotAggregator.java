package ru.yandex.practicum.telemetry.aggregator.snapshot;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Хранит снапшоты всех хабов и обновляет их по событиям датчиков.
 * Ничего не знает о Kafka: получает событие, возвращает изменившийся снапшот.
 */
@Component
public class SnapshotAggregator {

    /** hubId → снапшот состояния датчиков этого хаба */
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    /**
     * Обновляет снапшот хаба данными события.
     *
     * @return снапшот, если он изменился, иначе Optional.empty()
     */
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        // снапшот хаба: достаём существующий или создаём новый
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(event.getHubId(), hubId ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build());

        // если данные датчика уже есть — обновляем, только если событие новее и данные изменились
        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        if (oldState != null) {
            boolean isOlderEvent = oldState.getTimestamp().isAfter(event.getTimestamp());
            boolean isSameData = oldState.getData().equals(event.getPayload());
            if (isOlderEvent || isSameData) {
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}
