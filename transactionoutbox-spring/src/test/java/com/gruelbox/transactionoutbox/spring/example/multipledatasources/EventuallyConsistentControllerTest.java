package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer.Type;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.ComputerExternalQueueService;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.Employee;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.EmployeeExternalQueueService;
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

  @Autowired private JdbcTemplate employeeJdbcTemplate;

  @Autowired private JdbcTemplate computerJdbcTemplate;

  @Autowired private EmployeeExternalQueueService employeeExternalQueueService;
  @Autowired private ComputerExternalQueueService computerExternalQueueService;

  @BeforeEach
  void setUp() {
    this.restClient = RestClient.builder().baseUrl("http://localhost:" + port).build();
    employeeExternalQueueService.clear();
    computerExternalQueueService.clear();
  }

  @Test
  void testCheckNormalEmployees() throws InterruptedException {
    var joe = new Employee(1L, "Joe", "Strummer");
    var dave = new Employee(2L, "Dave", "Grohl");
    var neil = new Employee(3L, "Neil", "Diamond");
    var tupac = new Employee(4L, "Tupac", "Shakur");
    var jeff = new Employee(5L, "Jeff", "Mills");

    restClient.post().uri("/employee").body(joe).retrieve().toBodilessEntity();
    restClient.post().uri("/employee").body(dave).retrieve().toBodilessEntity();
    restClient.post().uri("/employee").body(neil).retrieve().toBodilessEntity();
    restClient.post().uri("/employee").body(tupac).retrieve().toBodilessEntity();
    restClient.post().uri("/employee").body(jeff).retrieve().toBodilessEntity();

    employeeJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + neil.getLastName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(
            () ->
                assertThat(employeeExternalQueueService.getSent())
                    .containsExactlyInAnyOrder(joe, dave, tupac, jeff));
  }

  @Test
  void testCheckNormalComputers() throws InterruptedException {
    var computerPc1 = new Computer(1L, "pc-001", Type.DESKTOP);
    var computerPc2 = new Computer(2L, "pc-002", Type.LAPTOP);
    var computerPc3 = new Computer(3L, "pc-003", Type.LAPTOP);
    var computerWebserver1 = new Computer(4L, "webserver-001", Type.SERVER);
    var computerWebserver2 = new Computer(5L, "webserver-002", Type.SERVER);

    restClient.post().uri("/computer").body(computerPc1).retrieve().toBodilessEntity();
    restClient.post().uri("/computer").body(computerPc2).retrieve().toBodilessEntity();
    restClient.post().uri("/computer").body(computerPc3).retrieve().toBodilessEntity();
    restClient.post().uri("/computer").body(computerWebserver1).retrieve().toBodilessEntity();
    restClient.post().uri("/computer").body(computerWebserver2).retrieve().toBodilessEntity();

    computerJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + computerPc3.getName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(
            () ->
                assertThat(computerExternalQueueService.getSent())
                    .containsExactlyInAnyOrder(
                        computerPc1, computerPc2, computerWebserver1, computerWebserver2));
  }

  @Test
  void testCheckOrderedEmployees() {

    var joe = new Employee(1L, "Joe", "Strummer");
    var dave = new Employee(2L, "Dave", "Grohl");
    var neil = new Employee(3L, "Neil", "Diamond");
    var tupac = new Employee(4L, "Tupac", "Shakur");
    var jeff = new Employee(5L, "Jeff", "Mills");

    restClient.post().uri("/employee?ordered=true").body(joe).retrieve().toBodilessEntity();
    restClient.post().uri("/employee?ordered=true").body(dave).retrieve().toBodilessEntity();
    restClient.post().uri("/employee?ordered=true").body(neil).retrieve().toBodilessEntity();
    restClient.post().uri("/employee?ordered=true").body(tupac).retrieve().toBodilessEntity();
    restClient.post().uri("/employee?ordered=true").body(jeff).retrieve().toBodilessEntity();

    employeeJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + neil.getLastName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(
            () -> assertThat(employeeExternalQueueService.getSent()).containsExactly(joe, dave));
  }

  @Test
  void testCheckOrderedComputers() {

    var computerPc1 = new Computer(1L, "pc-001", Type.DESKTOP);
    var computerPc2 = new Computer(2L, "pc-002", Type.LAPTOP);
    var computerPc3 = new Computer(3L, "pc-003", Type.LAPTOP);
    var computerWebserver1 = new Computer(4L, "webserver-001", Type.SERVER);
    var computerWebserver2 = new Computer(5L, "webserver-002", Type.SERVER);

    restClient.post().uri("/computer?ordered=true").body(computerPc1).retrieve().toBodilessEntity();
    restClient.post().uri("/computer?ordered=true").body(computerPc2).retrieve().toBodilessEntity();
    restClient.post().uri("/computer?ordered=true").body(computerPc3).retrieve().toBodilessEntity();
    restClient
        .post()
        .uri("/computer?ordered=true")
        .body(computerWebserver1)
        .retrieve()
        .toBodilessEntity();
    restClient
        .post()
        .uri("/computer?ordered=true")
        .body(computerWebserver2)
        .retrieve()
        .toBodilessEntity();

    employeeJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + computerPc3.getName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(
            () ->
                assertThat(computerExternalQueueService.getSent())
                    .containsExactly(computerPc1, computerPc2));
  }
}
