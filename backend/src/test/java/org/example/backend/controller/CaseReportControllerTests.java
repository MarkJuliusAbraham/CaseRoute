package org.example.backend.controller;

import org.example.backend.config.SecurityConfig;
import org.example.backend.model.dto.cases.CaseReportPage;
import org.example.backend.model.dto.cases.CaseStats;
import org.example.backend.service.CaseReportService;
import org.example.backend.service.CaseStatsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import java.util.UUID;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = CaseReportController.class, properties = "simulation.reports-enabled=true")
@Import(SecurityConfig.class)
class CaseReportControllerTests {
    @Autowired MockMvc mvc;
    @MockitoBean CaseReportService reports;
    @MockitoBean CaseStatsService stats;

    @Test void statsReturnPipelineCountsForTheRun() throws Exception {
        UUID id = UUID.randomUUID();
        given(stats.stats(id)).willReturn(new CaseStats(100, 30, 69, 1, 5));
        mvc.perform(get("/api/simulation/cases/stats").param("testRunId", id.toString()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.saved").value(100))
                .andExpect(jsonPath("$.routed").value(69)).andExpect(jsonPath("$.awaitingRelay").value(5));
    }

    @Test void ledgerStreamsTheRunsCasesToLocalCallersOnly() throws Exception {
        UUID id = UUID.randomUUID();
        doAnswer(call -> { ((java.io.Writer) call.getArgument(1)).write("a,OPEN\n"); return null; })
                .when(stats).writeLedger(eq(id), any());
        var started = mvc.perform(get("/api/simulation/cases/ledger").param("testRunId", id.toString())
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; }))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch(started))
                .andExpect(status().isOk()).andExpect(content().string("a,OPEN\n"));
        mvc.perform(get("/api/simulation/cases/ledger").param("testRunId", id.toString())
                        .with(request -> { request.setRemoteAddr("10.0.0.5"); return request; }))
                .andExpect(status().isForbidden());
    }

    @Test void filteredPageForwardsRunIdAndPagination() throws Exception {
        UUID id = UUID.randomUUID();
        given(reports.page(id, 2, 25)).willReturn(new CaseReportPage(List.of(), 2, 25, 60, 3));
        mvc.perform(get("/api/simulation/cases").param("testRunId", id.toString()).param("page", "2")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; }))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalElements").value(60))
                .andExpect(jsonPath("$.page").value(2));
        verify(reports).page(id, 2, 25);
    }

    @Test void allCasesUsesDefaultPageSize() throws Exception {
        given(reports.page(null, 0, 25)).willReturn(new CaseReportPage(List.of(), 0, 25, 0, 0));
        mvc.perform(get("/api/simulation/cases"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.cases").isEmpty());
        verify(reports).page(null, 0, 25);
    }

    @Test void oversizedPageIsRejected() throws Exception {
        mvc.perform(get("/api/simulation/cases").param("size", "101"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(reports);
    }

    @Test void remoteAccessIsRejected() throws Exception {
        mvc.perform(get("/api/simulation/cases")
                        .with(request -> { request.setRemoteAddr("192.0.2.1"); return request; }))
                .andExpect(status().isForbidden());
        verifyNoInteractions(reports);
    }
}
