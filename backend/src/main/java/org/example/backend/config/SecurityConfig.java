package org.example.backend.config;

import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.security.web.util.matcher.IpAddressMatcher;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;

@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http,
            @Value("${simulation.reset-enabled:false}") boolean resetEnabled,
            @Value("${simulation.reports-enabled:false}") boolean reportsEnabled) throws Exception {
        var submitCase = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.POST, "/api/cases");
        var createCaseDirectly = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.POST, "/api/cases/direct");
        var warmUp = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.POST, "/api/cases/warmup");
        var connectionTest = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.POST, "/api/cases/noop");
        RequestMatcher createCase = request -> submitCase.matches(request) || createCaseDirectly.matches(request)
                || warmUp.matches(request) || connectionTest.matches(request);
        // Client request IDs are random UUIDs known only to the submitter, so the ID itself grants access.
        var caseStatus = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.GET, "/api/cases/requests/{clientRequestId}");

        var resetPath = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.DELETE, "/api/simulation/cases");
        var ipv4Loopback = new IpAddressMatcher("127.0.0.0/8");
        var ipv6Loopback = new IpAddressMatcher("::1");
        // Pausing the downstream pipeline is a local experiment control, like the reset.
        var pipelinePath = PathPatternRequestMatcher.withDefaults().matcher("/api/simulation/pipeline");
        var sheddingPath = PathPatternRequestMatcher.withDefaults().matcher("/api/simulation/shedding");
        var kafkaPath = PathPatternRequestMatcher.withDefaults().matcher("/api/simulation/kafka/**");
        var downtimePath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.POST, "/api/simulation/downtime");
        RequestMatcher localReset = request -> resetEnabled
                && (resetPath.matches(request) || pipelinePath.matches(request) || sheddingPath.matches(request)
                    || kafkaPath.matches(request) || downtimePath.matches(request))
                && (ipv4Loopback.matches(request) || ipv6Loopback.matches(request));

        var reportPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases");
        var exportPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases/export");
        var statsPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases/stats");
        var ledgerPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases/ledger");
        RequestMatcher localReport = request -> reportsEnabled
                && (reportPath.matches(request) || exportPath.matches(request) || statsPath.matches(request)
                        || ledgerPath.matches(request))
                && (ipv4Loopback.matches(request) || ipv6Loopback.matches(request));

        http
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(createCase, caseStatus, localReset, localReport).permitAll()
                        .anyRequest().authenticated()
                )
                .csrf(csrf -> csrf.ignoringRequestMatchers(createCase, localReset));

        return http.build();
    }
}
