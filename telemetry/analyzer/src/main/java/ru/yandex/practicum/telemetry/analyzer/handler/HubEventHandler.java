package ru.yandex.practicum.telemetry.analyzer.handler;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/** Обработчик одного типа событий хаба (добавление/удаление устройства или сценария) */
public interface HubEventHandler {

    /** Класс payload, который умеет обрабатывать этот обработчик */
    Class<? extends SpecificRecordBase> getPayloadType();

    void handle(HubEventAvro event);
}
