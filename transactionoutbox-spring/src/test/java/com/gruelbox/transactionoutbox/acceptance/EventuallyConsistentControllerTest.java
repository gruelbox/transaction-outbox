package com.gruelbox.transactionoutbox.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.net.URL;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventuallyConsistentControllerTest {

  @LocalServerPort private int port;

  private URL base;

  @Inject private TestRestTemplate template;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
  }

  @Test
  void testCheck() throws Exception {
    ResponseEntity<String> response =
        template.getForEntity(base.toString() + "/createCustomer", String.class);
    assertThat("Done".equals(response.getBody()));

    for (int i = 0; i < 10; i++) {
      ResponseEntity<String> exists =
          template.getForEntity(base.toString() + "/gotEventAndCustomers", String.class);
      if ("Yes".equals(exists.getBody())) {
        return;
      }
      Thread.sleep(1000);
    }
    //noinspection ResultOfMethodCallIgnored
    fail("Could not confirm eventually consistent part of transaction completed");
  }
}
