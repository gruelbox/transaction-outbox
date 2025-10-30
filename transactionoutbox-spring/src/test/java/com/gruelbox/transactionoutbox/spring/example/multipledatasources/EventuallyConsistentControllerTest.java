package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer.Type;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.Employee;
import java.net.URL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventuallyConsistentControllerTest {

  @SuppressWarnings("unused")
  @LocalServerPort
  private int port;

  private URL base;

  @SuppressWarnings("unused")
  @Autowired
  private TestRestTemplate template;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
  }

  @Test
  void testCheckNormal() {

    var computerWinPc1 = new Computer(1L, "WinPC-001", Type.DESKTOP);
    var computerWinPc2 = new Computer(2L, "WinPC-002", Type.LAPTOP);

    var computerUrl = base.toString() + "/computer";
    assertTrue(template.postForEntity(computerUrl, computerWinPc1, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(computerUrl, computerWinPc2, Void.class).getStatusCode().is2xxSuccessful());

    var employeeJoe = new Employee(1L, "Joe", "Strummer");
    var employeeDave = new Employee(2L, "Dave", "Grohl");

    var employeeUrl = base.toString() + "/employee";
    assertTrue(template.postForEntity(employeeUrl, employeeJoe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(employeeUrl, employeeDave, Void.class).getStatusCode().is2xxSuccessful());
  }
}
