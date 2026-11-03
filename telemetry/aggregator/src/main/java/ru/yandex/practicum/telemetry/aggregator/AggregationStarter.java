package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.kafka.AggregatorKafkaProperties;
import ru.yandex.practicum.telemetry.aggregator.snapshot.SnapshotAggregator;

import java.util.List;
import java.util.Properties;

/**
 * Запускает агрегацию: читает события датчиков, обновляет снапшоты
 * и записывает изменившиеся снапшоты в Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final AggregatorKafkaProperties kafkaProperties;
    private final SnapshotAggregator snapshotAggregator;

    public void start() {
        KafkaConsumer<String, SensorEventAvro> consumer =
                new KafkaConsumer<>(toProperties(kafkaProperties.consumer()));
        KafkaProducer<String, SpecificRecordBase> producer =
                new KafkaProducer<>(toProperties(kafkaProperties.producer()));

        // при завершении приложения прерываем poll, чтобы корректно закрыть клиентов
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(kafkaProperties.topics().sensors()));

            // цикл опроса
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records =
                        consumer.poll(kafkaProperties.pollTimeout());

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    snapshotAggregator.updateState(record.value())
                            .ifPresent(snapshot -> sendSnapshot(producer, snapshot));
                }

                // фиксируем оффсеты после обработки пачки (at-least-once)
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
                    }
                });
            }

        } catch (WakeupException ignored) {
            // сигнал остановки — закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                // отправляем всё, что осталось в буфере продюсера,
                // и надёжно фиксируем оффсеты обработанных сообщений
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private void sendSnapshot(KafkaProducer<String, SpecificRecordBase> producer, SensorsSnapshotAvro snapshot) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                kafkaProperties.topics().snapshots(),
                null,
                snapshot.getTimestamp().toEpochMilli(),
                snapshot.getHubId(),
                snapshot);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить снапшот хаба {}", snapshot.getHubId(), exception);
            } else {
                log.debug("Снапшот хаба {} записан: партиция={}, офсет={}",
                        snapshot.getHubId(), metadata.partition(), metadata.offset());
            }
        });
    }

    private static Properties toProperties(AggregatorKafkaProperties.Client client) {
        Properties properties = new Properties();
        properties.putAll(client.properties());
        return properties;
    }
}
