package org.example.backend.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LoadSheddingTests {

    private static MockHttpServletRequest submission(String path) {
        var request = new MockHttpServletRequest("POST", path);
        request.setRequestURI(path);
        return request;
    }

    @Test
    void overTheLimitASubmissionIsRejectedAtOnceWithRetryAfter() throws Exception {
        var shedding = new LoadShedding();
        shedding.configure(true, 1);
        var second = new AtomicReference<MockHttpServletResponse>();
        var first = new MockHttpServletResponse();
        // While the first submission is still in progress, a second one arrives.
        shedding.doFilter(submission("/api/cases"), first, (req, res) -> {
            var response = new MockHttpServletResponse();
            shedding.doFilter(submission("/api/cases"), response, (r, s) -> fail("must not reach the controller"));
            second.set(response);
        });
        assertEquals(200, first.getStatus(), "the first one went through");
        assertEquals(503, second.get().getStatus());
        assertEquals("1", second.get().getHeader("Retry-After"), "Retry-After marks it as safe to resend");
        assertTrue(second.get().getContentAsString().contains("Busy"));
        assertEquals(1, shedding.state().shed());
        assertEquals(0, shedding.state().inFlight(), "slots are released when a submission finishes");
    }

    @Test
    void offByDefaultAndNeverAppliedToWarmUpsOrOtherPaths() throws Exception {
        var shedding = new LoadShedding();
        assertFalse(shedding.state().enabled());
        shedding.configure(true, 1);
        var reached = new int[1];
        shedding.doFilter(submission("/api/cases"), new MockHttpServletResponse(), (req, res) -> {
            for (String path : new String[] { "/api/cases/warmup", "/api/cases/requests/x" }) {
                var response = new MockHttpServletResponse();
                shedding.doFilter(submission(path), response, (r, s) -> reached[0]++);
                assertEquals(200, response.getStatus(), path + " is never shed");
            }
        });
        assertEquals(2, reached[0]);
    }

    @Test
    void limitMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new LoadShedding().configure(true, 0));
    }
}
