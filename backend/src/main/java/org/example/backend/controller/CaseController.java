package org.example.backend.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.backend.exception.CaseNotFoundException;
import org.example.backend.model.dto.cases.CaseResponse;
import org.example.backend.model.dto.cases.CaseStatusResponse;
import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.model.dto.cases.SubmissionAcceptedResponse;
import org.example.backend.service.CaseService;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

/**
 * Two write paths with one fixed contract each, so runs can compare them, plus a status lookup.
 * Both write paths are idempotent by clientRequestId, so a client may safely retry when it does not know
 * whether its earlier attempt succeeded.
 */
@RestController
@RequestMapping("/api/cases")
@RequiredArgsConstructor
public class CaseController {

    private final CaseService caseService;

    /**
     * Stores the submission in Kafka and replies 202; a consumer saves the case to Postgres later.
     * The Location header is where the client can look up the outcome.
     */
    @PostMapping
    public ResponseEntity<SubmissionAcceptedResponse> submitCase(@Valid @RequestBody CreateCaseRequest request) {
        SubmissionAcceptedResponse accepted = caseService.submitCase(request);
        return ResponseEntity.accepted().location(statusUrl(accepted.clientRequestId())).body(accepted);
    }

    /**
     * Saves the case to Postgres inside the request and replies 201 with the created case. A retry of a
     * submission that was already saved replies 200 with the existing case instead of creating a second one.
     */
    @PostMapping("/direct")
    public ResponseEntity<CaseResponse> createCaseDirectly(@Valid @RequestBody CreateCaseRequest request) {
        try {
            CaseResponse created = caseService.createCase(request);
            return ResponseEntity.created(URI.create("/api/cases/" + created.id())).body(created);
        } catch (DataIntegrityViolationException ex) {
            // The unique clientRequestId already exists: an earlier attempt was saved (its reply was lost).
            return caseService.findRetriedCase(request)
                    .map(existing -> ResponseEntity.ok().location(URI.create("/api/cases/" + existing.id())).body(existing))
                    .orElseThrow(() -> ex); // same ID, different content: still a 409 conflict
        }
    }

    /**
     * Warm-up for load tests: handled exactly like {@code POST /api/cases} (connection, security, JSON,
     * validation, and a Kafka write waiting for confirmation), but written to the {@code cases.warmup} topic,
     * which nothing consumes, so no case is ever created. Replies 204, or 503 if Kafka does not confirm.
     */
    @PostMapping("/warmup")
    public ResponseEntity<Void> warmUp(@Valid @RequestBody CreateCaseRequest request) {
        caseService.warmUp(request);
        return ResponseEntity.noContent().build();
    }

    /**
     * What happened to a submission: 200 with the case and its processing status once saved, 404 while no
     * case exists (never received, or accepted and still waiting in Kafka).
     */
    @GetMapping("/requests/{clientRequestId}")
    public CaseStatusResponse status(@PathVariable UUID clientRequestId) {
        return caseService.findStatus(clientRequestId)
                .orElseThrow(() -> CaseNotFoundException.forRequest(clientRequestId));
    }

    private static URI statusUrl(UUID clientRequestId) {
        return URI.create("/api/cases/requests/" + clientRequestId);
    }
}
