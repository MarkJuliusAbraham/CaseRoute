package org.example.generator.model;

/** Which HTTP client a run sends with. Outcomes are classified the same way for both. */
public enum HttpClientKind {
    /** Java's built-in java.net.http.HttpClient, spread over several instances. Collapses near 10,000 requests/s. */
    JDK,
    /** Jetty's HttpClient (the default): several instances, capped pre-opened connections used in rotation. */
    JETTY
}
