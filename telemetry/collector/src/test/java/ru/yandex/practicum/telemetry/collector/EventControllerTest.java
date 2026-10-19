package ru.yandex.practicum.telemetry.collector;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.telemetry.collector.controller.EventController;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.ScenarioAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.LightSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.CollectorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    CollectorService service;

    @Test
    void sensorEventIsDeserializedIntoConcreteSubclass() throws Exception {
        mvc.perform(post("/events/sensors").contentType(MediaType.APPLICATION_JSON).content("""
                        {"id":"sensor.light.3","hubId":"hub-2","timestamp":"2024-08-06T15:11:24.157Z",
                         "linkQuality":75,"luminosity":59,"type":"LIGHT_SENSOR_EVENT"}"""))
                .andExpect(status().isOk());

        ArgumentCaptor<SensorEvent> captor = ArgumentCaptor.forClass(SensorEvent.class);
        verify(service).collectSensorEvent(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(LightSensorEvent.class);
        assertThat(((LightSensorEvent) captor.getValue()).getLuminosity()).isEqualTo(59);
    }

    @Test
    void scenarioAddedEventIsAccepted() throws Exception {
        mvc.perform(post("/events/hubs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"hubId":"hub-1","type":"SCENARIO_ADDED","name":"Свет на кухне",
                         "conditions":[{"sensorId":"m-1","type":"MOTION","operation":"EQUALS","value":1}],
                         "actions":[{"sensorId":"lamp-1","type":"ACTIVATE"}]}"""))
                .andExpect(status().isOk());

        ArgumentCaptor<HubEvent> captor = ArgumentCaptor.forClass(HubEvent.class);
        verify(service).collectHubEvent(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(ScenarioAddedEvent.class);
        assertThat(captor.getValue().getTimestamp()).isNotNull();
    }

    @Test
    void invalidEventIsRejected() throws Exception {
        mvc.perform(post("/events/hubs").contentType(MediaType.APPLICATION_JSON).content("""
                        {"hubId":"","type":"DEVICE_REMOVED","id":"x"}"""))
                .andExpect(status().isBadRequest());
        verify(service, never()).collectHubEvent(any());
    }
}
