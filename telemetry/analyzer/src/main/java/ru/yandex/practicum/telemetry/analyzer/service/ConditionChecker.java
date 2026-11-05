package ru.yandex.practicum.telemetry.analyzer.service;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;

/** Проверяет одно условие сценария на данных датчика из снапшота */
@Component
public class ConditionChecker {

    public boolean check(Condition condition, SensorStateAvro state) {
        if (state == null || condition.getValue() == null) {
            return false; // датчика нет в снапшоте или нет опорного значения
        }
        Integer actual = extractValue(condition.getType(), state.getData());
        if (actual == null) {
            return false; // датчик не умеет измерять такую величину
        }
        int expected = condition.getValue();

        return switch (condition.getOperation()) {
            case EQUALS -> actual == expected;
            case GREATER_THAN -> actual > expected;
            case LOWER_THAN -> actual < expected;
        };
    }

    /** Достаёт из данных датчика величину нужного типа (логические значения -> 1/0) */
    private Integer extractValue(ConditionTypeAvro type, Object data) {
        return switch (type) {
            case MOTION -> data instanceof MotionSensorAvro motion ? toInt(motion.getMotion()) : null;
            case LUMINOSITY -> data instanceof LightSensorAvro light ? light.getLuminosity() : null;
            case SWITCH -> data instanceof SwitchSensorAvro sw ? toInt(sw.getState()) : null;
            case TEMPERATURE -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                if (data instanceof TemperatureSensorAvro temperature) {
                    yield temperature.getTemperatureC();
                }
                yield null;
            }
            case CO2LEVEL -> data instanceof ClimateSensorAvro climate ? climate.getCo2Level() : null;
            case HUMIDITY -> data instanceof ClimateSensorAvro climate ? climate.getHumidity() : null;
        };
    }

    private static int toInt(boolean value) {
        return value ? 1 : 0;
    }
}
