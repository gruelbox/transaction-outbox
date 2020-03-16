package com.gruelbox.transactionoutbox.demo;

import static org.assertj.core.api.Assertions.*;

import java.net.URL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TestEventuallyConsistentController {

  @LocalServerPort private int port;

  private URL base;

  @Autowired private TestRestTemplate template;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
  }

  @Test
  void check() throws Exception {
    ResponseEntity<String> response =
        template.getForEntity(base.toString() + "/createCustomer", String.class);
    assertThat("Done".equals(response.getBody()));

    for (int i = 0; i < 10; i++) {
      ResponseEntity<String> exists =
          template.getForEntity(base.toString() + "/gotEvent", String.class);
      if ("Yes".equals(exists.getBody())) {
        return;
      }
      Thread.sleep(1000);
    }
    fail("Could not confirm eventually consistent part of transaction completed");
  }
}
