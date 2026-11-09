package org.example.backend.model.dto.cases;

import java.util.List;

public record CaseReportPage(List<CaseResponse> cases, int page, int size, long totalElements, int totalPages) {}
