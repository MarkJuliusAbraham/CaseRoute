package org.example.backend.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex,
            HttpHeaders headers,
            HttpStatusCode status,
            WebRequest request) {
        Map<String, List<String>> errors = ex.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.groupingBy(
                        error -> error.getField(),
                        Collectors.mapping(
                                error -> error.getDefaultMessage() == null
                                        ? "Invalid value" : error.getDefaultMessage(),
                                Collectors.toList())
                ));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                status, "One or more request fields are invalid.");
        problem.setTitle("Validation failed");
        problem.setProperty("errors", errors);

        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @ExceptionHandler(CaseNotFoundException.class)
    public ProblemDetail handleCaseNotFound(CaseNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Case not found");
        return problem;
    }

    @ExceptionHandler(SubmissionUnavailableException.class)
    public ProblemDetail handleSubmissionUnavailable(SubmissionUnavailableException ex) {
        log.warn("Submission not acknowledged: {}", ex.getCause() == null ? ex.getMessage() : ex.getCause().toString());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage());
        problem.setTitle("Submission not accepted");
        return problem;
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ProblemDetail handleDataConflict(DataIntegrityViolationException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
                "The submission conflicts with existing data. Each clientRequestId must be unique.");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpectedException(Exception ex) {
        log.error("Unexpected error while processing request", ex);
        return ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred.");
    }
}
