package org.example.backend.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load shedding for submissions: when switched on, at most {@code maxConcurrent} submissions are worked on at once.
 * Any submission beyond that is rejected immediately with {@code 503 Service Unavailable} and {@code Retry-After: 1},
 * before parsing, security or any Kafka/database work, so a rejection costs almost nothing.
 *
 * <p>The {@code Retry-After} header is the contract: it means "busy, nothing was stored, safe to resend later".
 * A 503 without it (Kafka did not confirm a write) means the outcome is unknown.
 *
 * <p>Covers {@code POST /api/cases}, {@code /direct} and {@code /noop}; never the warm-up. Off by default, switched
 * at runtime through {@code PUT /api/simulation/shedding}; a restart turns it off.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class LoadShedding extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(LoadShedding.class);
    private static final Set<String> PATHS = Set.of("/api/cases", "/api/cases/direct", "/api/cases/noop");
    private static final byte[] BUSY = ("{\"title\":\"Busy\",\"status\":503,"
            + "\"detail\":\"The backend is at capacity. Nothing was stored; retry later.\"}").getBytes(StandardCharsets.UTF_8);

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong shed = new AtomicLong();
    private volatile boolean enabled;
    private volatile int maxConcurrent = 1000;

    public record State(boolean enabled, int maxConcurrent, int inFlight, long shed) { }

    public State state() { return new State(enabled, maxConcurrent, inFlight.get(), shed.get()); }

    public synchronized State configure(boolean enable, Integer limit) {
        if (limit != null) {
            if (limit < 1) throw new IllegalArgumentException("maxConcurrent must be at least 1");
            maxConcurrent = limit;
        }
        if (enable != enabled) log.info("Load shedding {} (max {} concurrent submissions)", enable ? "ON" : "off", maxConcurrent);
        enabled = enable;
        return state();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equals(request.getMethod()) || !PATHS.contains(request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!enabled) {
            chain.doFilter(request, response);
            return;
        }
        if (inFlight.incrementAndGet() > maxConcurrent) {
            inFlight.decrementAndGet();
            shed.incrementAndGet();
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.setHeader("Retry-After", "1");
            response.setContentType("application/problem+json");
            response.setContentLength(BUSY.length);
            response.getOutputStream().write(BUSY);
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            inFlight.decrementAndGet();
        }
    }
}
