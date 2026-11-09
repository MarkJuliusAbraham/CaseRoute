package org.example.backend.service;

import jakarta.persistence.EntityManager;
import org.example.backend.model.dto.cases.*;
import org.example.backend.model.entity.ServiceCase;
import org.example.backend.repository.CaseRepository;
import org.springframework.data.domain.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.Writer;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class CaseReportService {
    private final CaseRepository cases;
    private final EntityManager entities;

    public CaseReportService(CaseRepository cases, EntityManager entities) {
        this.cases = cases;
        this.entities = entities;
    }

    @Transactional(readOnly = true)
    public CaseReportPage page(UUID runId, int page, int size) {
        Page<ServiceCase> records = find(runId, page, size);
        return new CaseReportPage(records.map(CaseResponse::from).getContent(), page, size,
                records.getTotalElements(), records.getTotalPages());
    }

    private Page<ServiceCase> find(UUID runId, int page, int size) {
        var request = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt", "id"));
        return runId == null ? cases.findAll(request) : cases.findByTestRunId(runId, request);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public void export(UUID runId, Writer writer) throws IOException {
        writer.write("caseId,clientRequestId,testRunId,subject,status,createdAt\r\n");
        int index = 0;
        Page<ServiceCase> batch;
        do {
            batch = find(runId, index++, 1000);
            for (ServiceCase value : batch) {
                writer.write(Stream.of(value.getId(), value.getClientRequestId(), value.getTestRunId(),
                                value.getSubject(), value.getStatus(), value.getCreatedAt())
                        .map(CaseReportService::csvCell).collect(Collectors.joining(",")) + "\r\n");
            }
            writer.flush();
            entities.clear();
        } while (batch.hasNext());
    }

    static String csvCell(Object value) {
        String text = value == null ? "" : value.toString();
        if (!text.isEmpty() && "=+-@\t\r\n".indexOf(text.charAt(0)) >= 0) text = "'" + text;
        return "\"" + text.replace("\"", "\"\"") + "\"";
    }
}
