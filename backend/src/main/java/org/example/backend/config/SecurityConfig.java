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
        RequestMatcher createCase = request -> submitCase.matches(request) || createCaseDirectly.matches(request);

        var resetPath = PathPatternRequestMatcher.withDefaults()
                .matcher(HttpMethod.DELETE, "/api/simulation/cases");
        var ipv4Loopback = new IpAddressMatcher("127.0.0.0/8");
        var ipv6Loopback = new IpAddressMatcher("::1");
        RequestMatcher localReset = request -> resetEnabled && resetPath.matches(request)
                && (ipv4Loopback.matches(request) || ipv6Loopback.matches(request));

        var reportPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases");
        var exportPath = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.GET, "/api/simulation/cases/export");
        RequestMatcher localReport = request -> reportsEnabled
                && (reportPath.matches(request) || exportPath.matches(request))
                && (ipv4Loopback.matches(request) || ipv6Loopback.matches(request));

        http
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.GET, "/actuator/health").permitAll()
                        .requestMatchers(createCase, localReset, localReport).permitAll()
                        .anyRequest().authenticated()
                )
                .csrf(csrf -> csrf.ignoringRequestMatchers(createCase, localReset));

        return http.build();
    }
}
