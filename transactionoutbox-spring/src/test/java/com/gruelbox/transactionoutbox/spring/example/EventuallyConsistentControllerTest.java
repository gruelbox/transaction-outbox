package com.gruelbox.transactionoutbox.spring.example;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventuallyConsistentControllerTest {

  @SuppressWarnings("unused")
  @LocalServerPort
  private int port;

  private URL base;

  @SuppressWarnings("unused")
  @Autowired
  private TestRestTemplate template;

  @SuppressWarnings("unused")
  @Autowired
  private ExternalQueueService externalQueueService;

  @Autowired private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
    externalQueueService.clear();
  }

  @Test
  void testCheckNormal() {

    var joe = new Customer(1L, "Joe", "Strummer");
    var dave = new Customer(2L, "Dave", "Grohl");
    var neil = new Customer(3L, "Neil", "Diamond");
    var tupac = new Customer(4L, "Tupac", "Shakur");
    var jeff = new Customer(5L, "Jeff", "Mills");

    var url = base.toString() + "/customer";
    assertTrue(template.postForEntity(url, joe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class).getStatusCode().is2xxSuccessful());

    jdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + neil.getLastName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(
            () ->
                assertThat(externalQueueService.getSent())
                    .containsExactlyInAnyOrder(joe, dave, tupac, jeff));
  }

  @Test
  void testCheckOrdered() {

    var joe = new Customer(1L, "Joe", "Strummer");
    var dave = new Customer(2L, "Dave", "Grohl");
    var neil = new Customer(3L, "Neil", "Diamond");
    var tupac = new Customer(4L, "Tupac", "Shakur");
    var jeff = new Customer(5L, "Jeff", "Mills");

    var url = base.toString() + "/customer?ordered=true";
    assertTrue(template.postForEntity(url, joe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class).getStatusCode().is2xxSuccessful());

    jdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + neil.getLastName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(externalQueueService.getSent()).containsExactly(joe, dave));
  }
}
