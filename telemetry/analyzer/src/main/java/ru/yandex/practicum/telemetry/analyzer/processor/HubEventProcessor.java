package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.AnalyzerKafkaProperties;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Цикл опроса топика событий хабов (добавление/удаление устройств и сценариев).
 * Работает в отдельном потоке. Оффсеты фиксируются автоматически:
 * поток редкий, а повторная обработка таких событий безопасна.
 */
@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final AnalyzerKafkaProperties kafkaProperties;
    private final Map<Class<?>, HubEventHandler> handlers;

    public HubEventProcessor(AnalyzerKafkaProperties kafkaProperties, Set<HubEventHandler> handlers) {
        this.kafkaProperties = kafkaProperties;
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getPayloadType, Function.identity()));
    }

    @Override
    public void run() {
        KafkaConsumer<String, HubEventAvro> consumer =
                new KafkaConsumer<>(kafkaProperties.hubConsumer().toProperties());

        // при завершении приложения прерываем poll, чтобы корректно закрыть консьюмер
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(kafkaProperties.topics().hubs()));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(kafkaProperties.pollTimeout());

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handle(record.value());
                }
            }

        } catch (WakeupException ignored) {
            // сигнал остановки — закрываем консьюмер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хабов", e);
        } finally {
            log.info("Закрываем консьюмер событий хабов");
            consumer.close();
        }
    }

    private void handle(HubEventAvro event) {
        HubEventHandler handler = handlers.get(event.getPayload().getClass());
        if (handler == null) {
            log.warn("Нет обработчика для события хаба: {}", event.getPayload().getClass().getSimpleName());
            return;
        }
        try {
            handler.handle(event);
        } catch (Exception e) {
            // ошибка в одном событии не должна останавливать весь цикл опроса
            log.error("Не удалось обработать событие хаба {}", event, e);
        }
    }
}
