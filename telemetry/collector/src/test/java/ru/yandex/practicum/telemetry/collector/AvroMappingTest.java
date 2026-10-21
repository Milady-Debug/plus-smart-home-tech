package ru.yandex.practicum.telemetry.collector;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.telemetry.collector.kafka.AvroSerializer;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.telemetry.collector.model.hub.*;
import ru.yandex.practicum.telemetry.collector.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Проверяем полный путь: JSON-модель → Avro-объект → байты → обратно в Avro-объект. */
class AvroMappingTest {

    private final AvroSerializer serializer = new AvroSerializer();

    private <T> T roundTrip(org.apache.avro.specific.SpecificRecordBase record, Class<T> type) throws Exception {
        byte[] bytes = serializer.serialize("test", record);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(type);
        return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }

    @Test
    void climateSensorEventSurvivesRoundTrip() throws Exception {
        ClimateSensorEvent event = new ClimateSensorEvent();
        event.setId("climate-1");
        event.setHubId("hub-1");
        event.setTimestamp(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        event.setTemperatureC(22);
        event.setHumidity(45);
        event.setCo2Level(600);

        SensorEventAvro result = roundTrip(SensorEventMapper.toAvro(event), SensorEventAvro.class);

        assertThat(result.getHubId()).isEqualTo("hub-1");
        assertThat(result.getTimestamp()).isEqualTo(event.getTimestamp());
        assertThat(result.getPayload()).isInstanceOf(ClimateSensorAvro.class);
        assertThat(((ClimateSensorAvro) result.getPayload()).getCo2Level()).isEqualTo(600);
    }

    @Test
    void scenarioAddedEventSurvivesRoundTrip() throws Exception {
        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId("light-1");
        condition.setType(ConditionType.LUMINOSITY);
        condition.setOperation(ConditionOperation.LOWER_THAN);
        condition.setValue(50);

        DeviceAction action = new DeviceAction();
        action.setSensorId("lamp-1");
        action.setType(ActionType.ACTIVATE);

        ScenarioAddedEvent event = new ScenarioAddedEvent();
        event.setHubId("hub-1");
        event.setName("Свет на кухне");
        event.setConditions(List.of(condition));
        event.setActions(List.of(action));

        HubEventAvro result = roundTrip(HubEventMapper.toAvro(event), HubEventAvro.class);

        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) result.getPayload();
        assertThat(payload.getName()).isEqualTo("Свет на кухне");
        assertThat(payload.getConditions().get(0).getValue()).isEqualTo(50);
        assertThat(payload.getActions().get(0).getValue()).isNull();
    }
}
