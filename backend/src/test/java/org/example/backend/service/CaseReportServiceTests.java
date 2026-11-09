package org.example.backend.service;

import jakarta.persistence.EntityManager;
import org.example.backend.model.entity.ServiceCase;
import org.example.backend.repository.CaseRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.*;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CaseReportServiceTests {
    @Test void exportFiltersByRunAndEscapesSpreadsheetValues() throws Exception {
        var cases = mock(CaseRepository.class);
        var entities = mock(EntityManager.class);
        UUID runId = UUID.randomUUID();
        var record = new ServiceCase(UUID.randomUUID(), runId, "=SUM(1,2) \"quoted\"", "Description");
        when(cases.findByTestRunId(eq(runId), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(record)));
        var writer = new StringWriter();
        new CaseReportService(cases, entities).export(runId, writer);
        assertTrue(writer.toString().startsWith("caseId,clientRequestId,testRunId,subject,status,createdAt\r\n"));
        assertTrue(writer.toString().contains(runId.toString()));
        assertTrue(writer.toString().contains("\"'=SUM(1,2) \"\"quoted\"\"\""));
        verify(entities).clear();
        verify(cases, never()).findAll(any(Pageable.class));
    }
}
