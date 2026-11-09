package org.example.backend.controller;

import org.example.backend.config.SecurityConfig;
import org.example.backend.exception.SubmissionUnavailableException;
import org.example.backend.model.dto.cases.CaseResponse;
import org.example.backend.model.dto.cases.CaseStatusResponse;
import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.model.dto.cases.SubmissionAcceptedResponse;
import org.example.backend.model.enums.CaseStatus;
import org.example.backend.service.CaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@WebMvcTest(CaseController.class)
@Import(SecurityConfig.class)
class CaseControllerTests {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CaseService caseService;

    @Test
    void submitCaseWithoutAuthenticationOrCsrfReturnsAccepted() throws Exception {
        UUID clientRequestId = UUID.randomUUID();
        CreateCaseRequest request = new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice.");
        given(caseService.submitCase(request)).willReturn(new SubmissionAcceptedResponse(
                clientRequestId, null, SubmissionAcceptedResponse.ACCEPTED, Instant.parse("2026-11-08T12:00:00Z")));

        mockMvc.perform(post("/api/cases")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content(body(clientRequestId)))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/api/cases/requests/" + clientRequestId))
                .andExpect(jsonPath("$.clientRequestId").value(clientRequestId.toString()))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        verify(caseService).submitCase(request);
    }

    @Test
    void directModeSavesInsideTheRequestAndReturnsCreated() throws Exception {
        UUID id = UUID.randomUUID();
        UUID clientRequestId = UUID.randomUUID();
        CreateCaseRequest request = new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice.");
        given(caseService.createCase(request)).willReturn(new CaseResponse(id, clientRequestId, null,
                request.subject(), request.description(), CaseStatus.OPEN, Instant.parse("2026-11-08T12:00:00Z")));

        mockMvc.perform(post("/api/cases/direct")
                        .contentType(MediaType.APPLICATION_JSON).content(body(clientRequestId)))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/cases/" + id))
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("OPEN"));
        verify(caseService).createCase(request);
    }

    @Test
    void directEndpointValidatesLikeTheKafkaEndpoint() throws Exception {
        mockMvc.perform(post("/api/cases/direct").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"subject\":\"Duplicate charge\",\"description\":\"Two charges.\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.clientRequestId").exists());
        verifyNoInteractions(caseService);
    }

    @Test
    void unconfirmedKafkaWriteReturnsServiceUnavailable() throws Exception {
        UUID clientRequestId = UUID.randomUUID();
        given(caseService.submitCase(new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice.")))
                .willThrow(new SubmissionUnavailableException(new RuntimeException("broker down")));

        mockMvc.perform(post("/api/cases").contentType(MediaType.APPLICATION_JSON).content(body(clientRequestId)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.title").value("Submission not accepted"));
    }

    @Test
    void createCaseWithoutClientRequestIdReturnsBadRequest() throws Exception {
        mockMvc.perform(post("/api/cases")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"subject":"Duplicate charge","description":"Two charges."}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.clientRequestId").exists());
        verifyNoInteractions(caseService);
    }

    @Test
    void directRetryOfAnAlreadySavedCaseReturnsTheExistingCase() throws Exception {
        UUID id = UUID.randomUUID();
        UUID clientRequestId = UUID.randomUUID();
        CreateCaseRequest request = new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice.");
        given(caseService.createCase(request)).willThrow(new DataIntegrityViolationException("duplicate key"));
        given(caseService.findRetriedCase(request)).willReturn(Optional.of(new CaseResponse(id, clientRequestId, null,
                request.subject(), request.description(), CaseStatus.OPEN, Instant.parse("2026-11-08T12:00:00Z"))));

        mockMvc.perform(post("/api/cases/direct").contentType(MediaType.APPLICATION_JSON).content(body(clientRequestId)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()));
    }

    @Test
    void directSubmissionReusingAnIdWithDifferentContentIsAConflict() throws Exception {
        UUID clientRequestId = UUID.randomUUID();
        CreateCaseRequest request = new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice.");
        given(caseService.createCase(request)).willThrow(new DataIntegrityViolationException("duplicate key"));
        given(caseService.findRetriedCase(request)).willReturn(Optional.empty());

        mockMvc.perform(post("/api/cases/direct").contentType(MediaType.APPLICATION_JSON).content(body(clientRequestId)))
                .andExpect(status().isConflict());
    }

    @Test
    void warmUpWritesToKafkaLikeARealSubmissionAndRepliesNoContent() throws Exception {
        UUID clientRequestId = UUID.randomUUID();
        mockMvc.perform(post("/api/cases/warmup").contentType(MediaType.APPLICATION_JSON).content(body(clientRequestId)))
                .andExpect(status().isNoContent());
        verify(caseService).warmUp(new CreateCaseRequest(
                clientRequestId, null, "Duplicate charge", "The same purchase appears twice."));
    }

    @Test
    void invalidWarmUpIsRejectedBeforeReachingKafka() throws Exception {
        mockMvc.perform(post("/api/cases/warmup").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"subject\":\"Duplicate charge\",\"description\":\"Two charges.\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(caseService);
    }

    @Test
    void statusLookupReportsTheCaseOrNotFoundWithoutAuthentication() throws Exception {
        UUID clientRequestId = UUID.randomUUID();
        UUID caseId = UUID.randomUUID();
        given(caseService.findStatus(clientRequestId)).willReturn(Optional.of(new CaseStatusResponse(caseId,
                clientRequestId, null, CaseStatus.ROUTED, "BILLING", "Billing Support", 1,
                Instant.parse("2026-11-08T12:00:00Z"), Instant.parse("2026-11-08T12:00:02Z"))));

        mockMvc.perform(get("/api/cases/requests/" + clientRequestId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.caseId").value(caseId.toString()))
                .andExpect(jsonPath("$.status").value("ROUTED"))
                .andExpect(jsonPath("$.assignedTeam").value("Billing Support"));

        UUID unknown = UUID.randomUUID();
        given(caseService.findStatus(unknown)).willReturn(Optional.empty());
        mockMvc.perform(get("/api/cases/requests/" + unknown))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Case not found"));
    }

    private static String body(UUID clientRequestId) {
        return """
                {
                  "clientRequestId": "%s",
                  "subject": "Duplicate charge",
                  "description": "The same purchase appears twice."
                }
                """.formatted(clientRequestId);
    }
}
