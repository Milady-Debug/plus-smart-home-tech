package ru.yandex.practicum.telemetry.collector.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Обёртка над KafkaProducer. Один продюсер на всё приложение:
 * он потокобезопасен, а создавать его на каждый запрос дорого.
 */
@Slf4j
@Component
public class EventProducer {

    private final Producer<String, SpecificRecordBase> producer;

    public EventProducer(CollectorKafkaProperties kafkaProperties) {
        Properties config = new Properties();
        config.putAll(kafkaProperties.producer().properties());
        this.producer = new KafkaProducer<>(config);
    }

    /**
     * Отправка асинхронная: send() не ждёт ответа брокера, поэтому HTTP-запрос
     * обрабатывается быстро. Ключ — hubId: события одного хаба попадут в одну партицию
     * и сохранят порядок.
     */
    public void send(String topic, String key, Instant timestamp, SpecificRecordBase value) {
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие в топик {}: {}", topic, value, exception);
            } else {
                log.debug("Событие записано: топик={}, партиция={}, офсет={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @PreDestroy
    public void close() {
        // перед остановкой дописываем всё, что накопилось в буфере продюсера
        producer.flush();
        producer.close(Duration.ofSeconds(10));
    }
}
