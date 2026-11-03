package ru.yandex.practicum.telemetry.aggregator.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

/** Настройки из application.yaml, раздел aggregator.kafka */
@ConfigurationProperties(prefix = "aggregator.kafka")
public record AggregatorKafkaProperties(Topics topics,
                                        Duration pollTimeout,
                                        Client consumer,
                                        Client producer) {

    public record Topics(String sensors, String snapshots) {
    }

    /** Любые настройки клиента Kafka: bootstrap.servers, (де)сериализаторы и т.д. */
    public record Client(Map<String, String> properties) {
    }
}
