package ru.yandex.practicum.telemetry.analyzer.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return DeviceRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = ((DeviceRemovedEventAvro) event.getPayload()).getId();

        // если устройства уже нет — делать нечего
        sensorRepository.findByIdAndHubId(sensorId, hubId).ifPresent(sensor -> {
            // целостность данных: убираем из сценариев условия и действия с этим датчиком,
            // сценарий без условий или без действий теряет смысл — удаляем его
            for (Scenario scenario : scenarioRepository.findByHubId(hubId)) {
                scenario.getConditions().remove(sensorId);
                scenario.getActions().remove(sensorId);
                if (scenario.getConditions().isEmpty() || scenario.getActions().isEmpty()) {
                    scenarioRepository.delete(scenario);
                    log.info("Хаб {}: удалён сценарий '{}' — в нём не осталось условий или действий",
                            hubId, scenario.getName());
                }
            }
            // сначала удаляем ссылки на датчик в связующих таблицах, потом сам датчик
            scenarioRepository.flush();
            sensorRepository.delete(sensor);
            log.info("Хаб {}: удалено устройство {}", hubId, sensorId);
        });
    }
}
