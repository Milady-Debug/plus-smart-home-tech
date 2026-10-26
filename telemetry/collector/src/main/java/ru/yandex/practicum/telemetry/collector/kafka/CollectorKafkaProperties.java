package ru.yandex.practicum.telemetry.collector.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/** Настройки из application.yaml, раздел collector.kafka */
@ConfigurationProperties(prefix = "collector.kafka")
public record CollectorKafkaProperties(Topics topics, Producer producer) {

    public record Topics(String sensors, String hubs) {
    }

    /** Любые настройки KafkaProducer: bootstrap.servers, сериализаторы, acks и т.д. */
    public record Producer(Map<String, String> properties) {
    }
}
