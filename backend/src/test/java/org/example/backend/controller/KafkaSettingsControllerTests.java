package org.example.backend.controller;

import org.example.backend.config.SecurityConfig;
import org.example.backend.messaging.IntakeProducer;
import org.example.backend.messaging.KafkaTopics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = KafkaSettingsController.class, properties = "simulation.reset-enabled=true")
@Import(SecurityConfig.class)
class KafkaSettingsControllerTests {
    @Autowired MockMvc mvc;
    @MockitoBean IntakeProducer producer;
    @MockitoBean KafkaTopics topics;
    @MockitoBean org.example.backend.service.SimulationService simulation;

    private static final IntakeProducer.Settings DEFAULTS = new IntakeProducer.Settings(5, 16_384, "none", 5, 1);

    private static MockHttpServletRequestBuilder local(MockHttpServletRequestBuilder request) {
        return request.with(r -> { r.setRemoteAddr("127.0.0.1"); return r; });
    }

    @Test void readsProducerSettingsPartitionsAndConsumers() throws Exception {
        given(producer.settings()).willReturn(DEFAULTS);
        given(topics.partitions()).willReturn(Map.of("cases.submitted", 6));
        mvc.perform(local(get("/api/simulation/kafka")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.producer.lingerMs").value(5))
                .andExpect(jsonPath("$.producer.producers").value(1))
                .andExpect(jsonPath("$.partitions['cases.submitted']").value(6))
                .andExpect(jsonPath("$.sending").value(0));
    }

    @Test void changesDuringARunAreRefusedWithTheReason() throws Exception {
        given(producer.apply(any())).willThrow(new IllegalStateException("Submissions are being sent; change producer settings between runs"));
        mvc.perform(local(put("/api/simulation/kafka/producer")).contentType("application/json")
                        .content("{\"lingerMs\":20,\"batchSizeBytes\":65536,\"compression\":\"lz4\",\"maxInFlight\":5}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.detail").value("Submissions are being sent; change producer settings between runs"));

        given(producer.sending()).willReturn(3);
        mvc.perform(local(put("/api/simulation/kafka/partitions")).contentType("application/json")
                        .content("{\"topic\":\"cases.submitted\",\"partitions\":12}"))
                .andExpect(status().isConflict());
        verify(topics, never()).increase(any(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test void invalidSettingsAreBadRequests() throws Exception {
        given(producer.apply(any())).willThrow(new IllegalArgumentException("compression.type must be one of ..."));
        mvc.perform(local(put("/api/simulation/kafka/producer")).contentType("application/json")
                        .content("{\"lingerMs\":5,\"batchSizeBytes\":16384,\"compression\":\"brotli\",\"maxInFlight\":5}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("compression.type must be one of ..."));
    }

    @Test void onlyLocalCallersMayChangeKafka() throws Exception {
        mvc.perform(put("/api/simulation/kafka/producer").with(r -> { r.setRemoteAddr("10.0.0.5"); return r; })
                        .contentType("application/json")
                        .content("{\"lingerMs\":20,\"batchSizeBytes\":65536,\"compression\":\"lz4\",\"maxInFlight\":5}"))
                .andExpect(status().is4xxClientError());
        verify(producer, never()).apply(any());
    }
}
