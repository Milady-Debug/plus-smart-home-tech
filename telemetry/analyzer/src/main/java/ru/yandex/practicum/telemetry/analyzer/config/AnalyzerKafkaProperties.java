package ru.yandex.practicum.telemetry.analyzer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/** Настройки из application.yaml, раздел analyzer.kafka */
@ConfigurationProperties(prefix = "analyzer.kafka")
public record AnalyzerKafkaProperties(Topics topics,
                                      Duration pollTimeout,
                                      Client hubConsumer,
                                      Client snapshotConsumer) {

    public record Topics(String hubs, String snapshots) {
    }

    /** Любые настройки консьюмера Kafka: bootstrap.servers, group.id, десериализаторы и т.д. */
    public record Client(Map<String, String> properties) {

        public Properties toProperties() {
            Properties result = new Properties();
            result.putAll(properties);
            return result;
        }
    }
}
