package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;

import java.time.Instant;

/** gRPC-клиент Hub Router: отправляет хабу команды для устройств */
@Slf4j
@Service
public class HubRouterClient {

    private final HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router")
                           HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    /** Отправляет на исполнение все действия сработавшего сценария */
    public void executeActions(Scenario scenario) {
        scenario.getActions().forEach((sensorId, action) -> {
            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(toProto(sensorId, action))
                    .setTimestamp(toTimestamp(Instant.now()))
                    .build();
            try {
                hubRouterClient.handleDeviceAction(request);
                log.info("Хаб {}: сценарий '{}' -> {} для устройства {}",
                        scenario.getHubId(), scenario.getName(), action.getType(), sensorId);
            } catch (Exception e) {
                log.error("Не удалось отправить действие хабу {}: {}", scenario.getHubId(), request, e);
            }
        });
    }

    private DeviceActionProto toProto(String sensorId, Action action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action.getType().name()));
        if (action.getValue() != null) {
            builder.setValue(action.getValue()); // поле optional — заполняем, только если значение есть
        }
        return builder.build();
    }

    private Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
