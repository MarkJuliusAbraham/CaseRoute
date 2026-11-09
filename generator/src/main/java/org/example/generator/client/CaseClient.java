package org.example.generator.client;

import org.example.generator.model.Submission;
import org.example.generator.model.SubmissionMode;
import org.example.generator.model.SubmissionResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

@Component
public class CaseClient {
    // HTTP/1.1 explicitly: the default (HTTP/2) adds an upgrade attempt to every new connection,
    // which the HTTP/1.1 backend ignores. Pinning it skips that wasted work.
    private final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper json;
    private final URI target;

    public CaseClient(ObjectMapper json, @Value("${generator.target-url}") URI target) {
        this.json = json;
        this.target = target;
    }

    public SubmissionResult send(Submission submission, double schedulingDelayMs) {
        return send(submission, schedulingDelayMs, SubmissionMode.KAFKA);
    }

    public SubmissionResult send(Submission submission, double schedulingDelayMs, SubmissionMode mode) {
        long start = System.nanoTime();
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(target + mode.pathSuffix()))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(submission)))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            int expected = mode == SubmissionMode.DIRECT ? 201 : 202;
            if (status != expected) {
                return result(submission, status >= 500 ? "UNKNOWN" : "REJECTED", status, null,
                        start, schedulingDelayMs, "HTTP " + status);
            }
            try {
                var body = json.readTree(response.body());
                boolean sameRequest = submission.clientRequestId().toString().equals(body.path("clientRequestId").asText())
                        && submission.testRunId().toString().equals(body.path("testRunId").asText());
                if (mode == SubmissionMode.DIRECT) {
                    // Direct: the reply is the saved case, so its id and payload can be checked.
                    String id = body.path("id").asText("");
                    java.util.UUID.fromString(id);
                    boolean matches = sameRequest
                            && submission.subject().equals(body.path("subject").asText())
                            && submission.description().equals(body.path("description").asText())
                            && "OPEN".equals(body.path("status").asText());
                    return result(submission, matches ? "ACCEPTED" : "MISMATCH", status, id,
                            start, schedulingDelayMs, matches ? "Creation response matched" : "Unexpected response fields");
                }
                boolean matches = sameRequest && "ACCEPTED".equals(body.path("status").asText());
                return result(submission, matches ? "ACCEPTED" : "MISMATCH", status, null,
                        start, schedulingDelayMs, matches ? "Acceptance response matched" : "Unexpected response fields");
            } catch (Exception ex) {
                return result(submission, "MISMATCH", status, null, start, schedulingDelayMs,
                        "Invalid " + (mode == SubmissionMode.DIRECT ? "creation" : "acceptance") + " response");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return result(submission, "UNKNOWN", null, null, start, schedulingDelayMs,
                    "Interrupted; database outcome unknown");
        } catch (Exception ex) {
            return result(submission, "UNKNOWN", null, null, start, schedulingDelayMs,
                    ex.getClass().getSimpleName() + "; database outcome unknown");
        }
    }

    private SubmissionResult result(Submission submission, String outcome, Integer status, String caseId,
            long start, double delay, String detail) {
        return new SubmissionResult(submission.clientRequestId(), outcome, status, caseId,
                (System.nanoTime() - start) / 1_000_000.0, delay, detail, Instant.now());
    }
}
