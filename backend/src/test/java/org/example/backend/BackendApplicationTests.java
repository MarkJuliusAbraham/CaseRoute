package org.example.backend;

import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BackendApplicationTests {
    @Autowired WebServerApplicationContext context;

    @Autowired org.springframework.core.env.Environment environment;

    @Test
    void contextLoads() {
    }

    /** A normal stop lets requests in progress finish and reply, so a planned restart leaves none in doubt. */
    @Test
    void shutdownIsGraceful() {
        assertEquals("graceful", environment.getProperty("server.shutdown"));
        assertEquals("20s", environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase"));
    }

    /** The generator keeps a fixed pool of connections, so Tomcat must never close one for being idle. */
    @Test
    void idleConnectionsAreNeverClosedAndKeepAliveIsUnlimited() {
        var tomcat = ((TomcatWebServer) context.getWebServer()).getTomcat();
        var protocol = (AbstractHttp11Protocol<?>) tomcat.getConnector().getProtocolHandler();
        assertEquals(-1, protocol.getKeepAliveTimeout());
        assertEquals(-1, protocol.getConnectionTimeout());
        assertEquals(-1, protocol.getMaxKeepAliveRequests());
    }
}
