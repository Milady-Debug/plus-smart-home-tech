package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.kafka.EventProducer;
import ru.yandex.practicum.telemetry.collector.kafka.CollectorKafkaProperties;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectorService {

    private final EventProducer producer;
    private final CollectorKafkaProperties kafkaProperties;

    public void collectSensorEvent(SensorEvent event) {
        log.debug("Событие датчика: {}", event);
        producer.send(kafkaProperties.topics().sensors(),
                event.getHubId(),
                event.getTimestamp(),
                SensorEventMapper.toAvro(event));
    }

    public void collectHubEvent(HubEvent event) {
        log.debug("Событие хаба: {}", event);
        producer.send(kafkaProperties.topics().hubs(),
                event.getHubId(),
                event.getTimestamp(),
                HubEventMapper.toAvro(event));
    }
}
