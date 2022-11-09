package com.synaos.transactionoutbox.acceptance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

import javax.inject.Inject;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventuallyConsistentControllerTest {

    @LocalServerPort
    private int port;

    private URL base;

    @Inject
    private TestRestTemplate template;

    @BeforeEach
    void setUp() throws Exception {
        this.base = new URL("http://localhost:" + port + "/");
    }

    @Test
    void testCheck() throws Exception {
        ResponseEntity<String> response =
                template.getForEntity(base.toString() + "/createCustomer", String.class);
        assertThat("Done").isEqualTo(response.getBody());

        for (int i = 0; i < 10; i++) {
            ResponseEntity<String> exists =
                    template.getForEntity(base.toString() + "/gotEventAndCustomers", String.class);
            if ("Yes".equals(exists.getBody())) {
                return;
            }
            Thread.sleep(1000);
        }
        fail("Could not confirm eventually consistent part of transaction completed");
    }
}
