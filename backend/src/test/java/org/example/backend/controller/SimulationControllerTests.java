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
