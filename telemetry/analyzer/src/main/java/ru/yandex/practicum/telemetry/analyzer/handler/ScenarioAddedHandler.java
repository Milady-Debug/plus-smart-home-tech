package ru.yandex.practicum.telemetry.analyzer.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioAddedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();

        // все датчики из условий и действий должны быть зарегистрированы в этом хабе
        Set<String> sensorIds = new HashSet<>();
        payload.getConditions().forEach(condition -> sensorIds.add(condition.getSensorId()));
        payload.getActions().forEach(action -> sensorIds.add(action.getSensorId()));
        if (sensorRepository.countByIdInAndHubId(sensorIds, hubId) != sensorIds.size()) {
            log.warn("Хаб {}: сценарий '{}' не сохранён — не все устройства зарегистрированы в хабе",
                    hubId, payload.getName());
            return;
        }

        // сценарий с таким именем уже есть — заменяем его новой версией
        scenarioRepository.findByHubIdAndName(hubId, payload.getName()).ifPresent(old -> {
            scenarioRepository.delete(old);
            scenarioRepository.flush();
        });

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(payload.getName());
        payload.getConditions().forEach(condition ->
                scenario.getConditions().put(condition.getSensorId(), toCondition(condition)));
        payload.getActions().forEach(action ->
                scenario.getActions().put(action.getSensorId(), toAction(action)));

        scenarioRepository.save(scenario);
        log.info("Хаб {}: сохранён сценарий '{}'", hubId, payload.getName());
    }

    private Condition toCondition(ScenarioConditionAvro condition) {
        return Condition.builder()
                .type(condition.getType())
                .operation(condition.getOperation())
                .value(toInteger(condition.getValue()))
                .build();
    }

    private Action toAction(DeviceActionAvro action) {
        return Action.builder()
                .type(action.getType())
                .value(action.getValue())
                .build();
    }

    /** В Avro значение условия — union { null, int, boolean }; в БД храним как число */
    private Integer toInteger(Object value) {
        if (value instanceof Integer intValue) {
            return intValue;
        }
        if (value instanceof Boolean boolValue) {
            return boolValue ? 1 : 0;
        }
        return null;
    }
}
