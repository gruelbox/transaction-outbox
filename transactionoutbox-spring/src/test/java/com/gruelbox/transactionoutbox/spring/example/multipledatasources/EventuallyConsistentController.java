package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.ComputerExternalQueueService;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.ComputerRepository;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.Employee;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.EmployeeExternalQueueService;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@SuppressWarnings("unused")
@RestController
public class EventuallyConsistentController {

  @Autowired private ComputerRepository computerRepository;

  @Autowired private TransactionOutbox computerTransactionOutbox;

  @Autowired private EmployeeRepository employeeRepository;

  @Autowired private TransactionOutbox employeeTransactionOutbox;

  @SuppressWarnings("SameReturnValue")
  @PostMapping("/computer")
  @Transactional(transactionManager = "computerTransactionManager")
  public void createComputer(
      @RequestBody Computer computer,
      @RequestParam(name = "ordered", required = false) Boolean ordered) {

    computerRepository.save(computer);
    if (ordered != null && ordered) {
      computerTransactionOutbox
          .with()
          .ordered("justonetopic")
          .schedule(ComputerExternalQueueService.class)
          .sendComputerCreatedEvent(computer);
    } else {
      computerTransactionOutbox
          .schedule(ComputerExternalQueueService.class)
          .sendComputerCreatedEvent(computer);
    }
  }

  @GetMapping("/computer/{id}")
  public Computer getComputer(@PathVariable long id) {
    return computerRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }

  @SuppressWarnings("SameReturnValue")
  @PostMapping("/employee")
  @Transactional(transactionManager = "employeeTransactionManager")
  public void createEmployee(
      @RequestBody Employee employee,
      @RequestParam(name = "ordered", required = false) Boolean ordered) {

    employeeRepository.save(employee);
    if (ordered != null && ordered) {
      employeeTransactionOutbox
          .with()
          .ordered("justonetopic")
          .schedule(EmployeeExternalQueueService.class)
          .sendEmployeeCreatedEvent(employee);
    } else {
      employeeTransactionOutbox
          .schedule(EmployeeExternalQueueService.class)
          .sendEmployeeCreatedEvent(employee);
    }
  }

  @GetMapping("/employee/{id}")
  public Employee getEmployee(@PathVariable long id) {
    return employeeRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }
}
