package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer.Type;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.ComputerExternalQueueService;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.Employee;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.EmployeeExternalQueueService;
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

  @Autowired
  private JdbcTemplate employeeJdbcTemplate;

  @Autowired
  private JdbcTemplate computerJdbcTemplate;

  @Autowired
  private EmployeeExternalQueueService employeeExternalQueueService;
  @Autowired
  private ComputerExternalQueueService computerExternalQueueService;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
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

    var url = base.toString() + "/employee";
    assertTrue(template.postForEntity(url, joe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class).getStatusCode().is2xxSuccessful());

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

    var computerUrl = base.toString() + "/computer";
    assertTrue(template.postForEntity(computerUrl, computerPc1, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(computerUrl, computerPc2, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(computerUrl, computerPc3, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(computerUrl, computerWebserver1, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(computerUrl, computerWebserver2, Void.class).getStatusCode().is2xxSuccessful());

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
                    .containsExactlyInAnyOrder(computerPc1, computerPc2, computerWebserver1, computerWebserver2));
  }

  @Test
  void testCheckOrderedEmployees() {

    var joe = new Employee(1L, "Joe", "Strummer");
    var dave = new Employee(2L, "Dave", "Grohl");
    var neil = new Employee(3L, "Neil", "Diamond");
    var tupac = new Employee(4L, "Tupac", "Shakur");
    var jeff = new Employee(5L, "Jeff", "Mills");

    var url = base.toString() + "/employee?ordered=true";
    assertTrue(template.postForEntity(url, joe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class).getStatusCode().is2xxSuccessful());

    employeeJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + neil.getLastName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(employeeExternalQueueService.getSent()).containsExactly(joe, dave));
  }

  @Test
  void testCheckOrderedComputers() {

    var computerPc1 = new Computer(1L, "pc-001", Type.DESKTOP);
    var computerPc2 = new Computer(2L, "pc-002", Type.LAPTOP);
    var computerPc3 = new Computer(3L, "pc-003", Type.LAPTOP);
    var computerWebserver1 = new Computer(4L, "webserver-001", Type.SERVER);
    var computerWebserver2 = new Computer(5L, "webserver-002", Type.SERVER);

    var url = base.toString() + "/computer?ordered=true";
    assertTrue(template.postForEntity(url, computerPc1, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, computerPc2, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, computerPc3, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, computerWebserver1, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, computerWebserver2, Void.class).getStatusCode().is2xxSuccessful());

    employeeJdbcTemplate.execute(
        "UPDATE txno_outbox SET invocation='non-deserializable invocation' WHERE invocation LIKE '%"
            + computerPc3.getName()
            + "%'");

    await()
        .atMost(10, SECONDS)
        .pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(computerExternalQueueService.getSent()).containsExactly(computerPc1, computerPc2));
  }
}
