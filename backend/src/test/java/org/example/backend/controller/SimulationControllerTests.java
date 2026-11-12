package org.example.backend.controller;

import org.example.backend.config.SecurityConfig;
import org.example.backend.service.SimulationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = SimulationController.class, properties = "simulation.reset-enabled=true")
@Import(SecurityConfig.class)
class SimulationControllerTests {
    @Autowired MockMvc mvc;
    @MockitoBean SimulationService simulation;
    @MockitoBean org.example.backend.service.PipelineControl pipeline;
    @MockitoBean org.example.backend.service.ShutdownControl shutdown;

    @Test void downtimeTestsNeedConfirmationAndALocalCaller() throws Exception {
        var request = org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/simulation/downtime");
        mvc.perform(request.with(r -> { r.setRemoteAddr("127.0.0.1"); return r; })
                        .contentType("application/json").content("{\"mode\":\"crash\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/simulation/downtime")
                        .with(r -> { r.setRemoteAddr("10.0.0.5"); return r; }).header("X-Confirm-Downtime", "TAKE_BACKEND_DOWN")
                        .contentType("application/json").content("{\"mode\":\"crash\"}"))
                .andExpect(status().is4xxClientError());
        verifyNoInteractions(shutdown);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/simulation/downtime")
                        .with(r -> { r.setRemoteAddr("127.0.0.1"); return r; }).header("X-Confirm-Downtime", "TAKE_BACKEND_DOWN")
                        .contentType("application/json").content("{\"mode\":\"graceful\"}"))
                .andExpect(status().isAccepted());
        verify(shutdown).graceful();
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/simulation/downtime")
                        .with(r -> { r.setRemoteAddr("127.0.0.1"); return r; }).header("X-Confirm-Downtime", "TAKE_BACKEND_DOWN")
                        .contentType("application/json").content("{\"mode\":\"sideways\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test void localCallersCanPauseResumeAndReadTheDownstreamPipeline() throws Exception {
        org.mockito.BDDMockito.given(pipeline.isPaused()).willReturn(true);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/simulation/pipeline")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .contentType("application/json").content("{\"paused\":true}"))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.paused").value(true));
        verify(pipeline).setPaused(true);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/api/simulation/pipeline")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; }))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.paused").value(true));
    }

    @Test void localCallersCanSwitchLoadSheddingAndReadItsState() throws Exception {
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/simulation/shedding")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .contentType("application/json").content("{\"enabled\":true,\"maxConcurrent\":500}"))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.enabled").value(true))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.maxConcurrent").value(500));
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/simulation/shedding")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .contentType("application/json").content("{\"enabled\":true,\"maxConcurrent\":0}"))
                .andExpect(status().isBadRequest());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/simulation/shedding")
                        .with(request -> { request.setRemoteAddr("10.0.0.5"); return request; })
                        .contentType("application/json").content("{\"enabled\":false}"))
                .andExpect(status().isForbidden());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/api/simulation/shedding")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; }))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.enabled").value(true));
    }

    @Test void remoteCallersCannotPauseThePipeline() throws Exception {
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/simulation/pipeline")
                        .with(request -> { request.setRemoteAddr("10.0.0.5"); return request; })
                        .contentType("application/json").content("{\"paused\":true}"))
                .andExpect(status().isForbidden());
        org.mockito.Mockito.verify(pipeline, org.mockito.Mockito.never()).setPaused(org.mockito.ArgumentMatchers.anyBoolean());
    }

    @Test void confirmedLocalResetReturnsNoContent() throws Exception {
        mvc.perform(delete("/api/simulation/cases")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .header("X-Confirm-Reset", "DELETE_ALL_CASES"))
                .andExpect(status().isNoContent());
        verify(simulation).clearCases();
    }

    @Test void missingConfirmationCannotDeleteCases() throws Exception {
        mvc.perform(delete("/api/simulation/cases")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; }))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(simulation);
    }

    @Test void incorrectConfirmationCannotDeleteCases() throws Exception {
        mvc.perform(delete("/api/simulation/cases")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .header("X-Confirm-Reset", "WRONG"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(simulation);
    }

    @Test void remoteClientCannotResetCases() throws Exception {
        mvc.perform(delete("/api/simulation/cases")
                        .with(request -> { request.setRemoteAddr("192.0.2.1"); return request; })
                        .header("X-Confirm-Reset", "DELETE_ALL_CASES"))
                .andExpect(status().isForbidden());
        verifyNoInteractions(simulation);
    }
}
