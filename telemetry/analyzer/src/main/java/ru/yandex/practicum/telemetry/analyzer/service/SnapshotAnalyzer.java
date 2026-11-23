package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.Map;

/** Проверяет сценарии хаба на очередном снапшоте и запускает сработавшие */
@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalyzer {

    private final ScenarioRepository scenarioRepository;
    private final ConditionChecker conditionChecker;
    private final HubRouterClient hubRouterClient;

    public void analyze(SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> states = snapshot.getSensorsState();

        // filter -> forEach: оставляем сценарии, у которых выполнены все условия, и исполняем их
        scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> isTriggered(scenario, states))
                .peek(scenario -> log.debug("Хаб {}: сработал сценарий '{}'",
                        scenario.getHubId(), scenario.getName()))
                .forEach(hubRouterClient::executeActions);
    }

    private boolean isTriggered(Scenario scenario, Map<String, SensorStateAvro> states) {
        return !scenario.getConditions().isEmpty()
                && scenario.getConditions().entrySet().stream()
                .allMatch(entry -> conditionChecker.check(entry.getValue(), states.get(entry.getKey())));
    }
}
