package ru.yandex.practicum.telemetry.analyzer.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String name = ((ScenarioRemovedEventAvro) event.getPayload()).getName();

        // условия и действия удалятся вместе со сценарием (cascade + orphanRemoval)
        scenarioRepository.findByHubIdAndName(event.getHubId(), name).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            log.info("Хаб {}: удалён сценарий '{}'", event.getHubId(), name);
        });
    }
}
