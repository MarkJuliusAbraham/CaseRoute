package org.example.backend.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.backend.model.dto.cases.CaseResponse;
import org.example.backend.model.dto.cases.CreateCaseRequest;
import org.example.backend.model.dto.cases.SubmissionAcceptedResponse;
import org.example.backend.service.CaseService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

/** Two write paths with one fixed contract each, so runs can compare them. */
@RestController
@RequestMapping("/api/cases")
@RequiredArgsConstructor
public class CaseController {

    private final CaseService caseService;

    /** Stores the submission in Kafka and replies 202; a consumer saves the case to Postgres later. */
    @PostMapping
    public ResponseEntity<SubmissionAcceptedResponse> submitCase(@Valid @RequestBody CreateCaseRequest request) {
        return ResponseEntity.accepted().body(caseService.submitCase(request));
    }

    /** Saves the case to Postgres inside the request and replies 201 with the created case. */
    @PostMapping("/direct")
    public ResponseEntity<CaseResponse> createCaseDirectly(@Valid @RequestBody CreateCaseRequest request) {
        CaseResponse created = caseService.createCase(request);
        return ResponseEntity.created(URI.create("/api/cases/" + created.id())).body(created);
    }
}
