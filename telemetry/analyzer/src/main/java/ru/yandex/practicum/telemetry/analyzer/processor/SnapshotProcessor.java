package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerKafkaProperties;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotAnalyzer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Цикл опроса топика снапшотов. Работает в основном потоке.
 * Повторная обработка снапшотов нежелательна (действия выполнятся дважды),
 * поэтому оффсет фиксируется вручную сразу после обработки каждого снапшота.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final AnalyzerKafkaProperties kafkaProperties;
    private final SnapshotAnalyzer snapshotAnalyzer;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        KafkaConsumer<String, SensorsSnapshotAvro> consumer =
                new KafkaConsumer<>(kafkaProperties.snapshotConsumer().toProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(kafkaProperties.topics().snapshots()));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(kafkaProperties.pollTimeout());

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        snapshotAnalyzer.analyze(record.value());
                    } catch (Exception e) {
                        log.error("Ошибка при анализе снапшота хаба {}", record.key(), e);
                    }
                    commitOffset(consumer, record);
                }
            }

        } catch (WakeupException ignored) {
            // сигнал остановки — закрываем консьюмер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшотов", e);
        } finally {
            try {
                // надёжно фиксируем оффсеты всех обработанных снапшотов
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер снапшотов");
                consumer.close();
            }
        }
    }

    private void commitOffset(KafkaConsumer<String, SensorsSnapshotAvro> consumer,
                              ConsumerRecord<String, SensorsSnapshotAvro> record) {
        // фиксируем «следующее» сообщение: offset + 1
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));

        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
            if (exception != null) {
                log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
            }
        });
    }
}
