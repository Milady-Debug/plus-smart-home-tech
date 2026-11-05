package ru.yandex.practicum.telemetry.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Главный класс сервиса Aggregator.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class Aggregator {

    public static void main(String[] args) {
        // run возвращает настроенный контекст со всеми бинами приложения
        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);

        // сервера у Aggregator нет, поэтому «двигатель» — цикл опроса — запускаем сами
        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        aggregator.start();
    }
}
