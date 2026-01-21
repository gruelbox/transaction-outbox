package com.gruelbox.transactionoutbox.spring.example.simple;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventuallyConsistentControllerTest {

  @LocalServerPort private int port;

  private RestClient restClient;

  @Autowired private ExternalQueueService externalQueueService;

  @Autowired private JdbcTemplate jdbcTemplate;

  @BeforeEach
  void setUp() {
    this.restClient = RestClient.builder().baseUrl("http://localhost:" + port).build();
    externalQueueService.clear();
  }

  @Test
  void testCheckNormal() {

    var joe = new Customer(1L, "Joe", "Strummer");
    var dave = new Customer(2L, "Dave", "Grohl");
    var neil = new Customer(3L, "Neil", "Diamond");
    var tupac = new Customer(4L, "Tupac", "Shakur");
    var jeff = new Customer(5L, "Jeff", "Mills");

    restClient.post().uri("/customer").body(joe).retrieve().toBodilessEntity();
    restClient.post().uri("/customer").body(dave).retrieve().toBodilessEntity();
    restClient.post().uri("/customer").body(neil).retrieve().toBodilessEntity();
    restClient.post().uri("/customer").body(tupac).retrieve().toBodilessEntity();
    restClient.post().uri("/customer").body(jeff).retrieve().toBodilessEntity();

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

    restClient.post().uri("/customer?ordered=true").body(joe).retrieve().toBodilessEntity();
    restClient.post().uri("/customer?ordered=true").body(dave).retrieve().toBodilessEntity();
    restClient.post().uri("/customer?ordered=true").body(neil).retrieve().toBodilessEntity();
    restClient.post().uri("/customer?ordered=true").body(tupac).retrieve().toBodilessEntity();
    restClient.post().uri("/customer?ordered=true").body(jeff).retrieve().toBodilessEntity();

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
