package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.Computer;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer.ComputerRepository;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.Employee;
import com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@SuppressWarnings("unused")
@RestController
public class EventuallyConsistentController {

  @Autowired
  private ComputerRepository computerRepository;

  @Autowired
  private EmployeeRepository employeeRepository;

  @SuppressWarnings("SameReturnValue")
  @PostMapping("/computer")
  @Transactional(transactionManager = "computerTransactionManager")
  public void createCustomer(
      @RequestBody Computer computer) {
    computerRepository.save(computer);
  }

  @GetMapping("/computer/{id}")
  public Computer getCustomer(@PathVariable long id) {
    return computerRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }

  @SuppressWarnings("SameReturnValue")
  @PostMapping("/employee")
  @Transactional(transactionManager = "employeeTransactionManager")
  public void createEmployee(
      @RequestBody Employee employee) {
    employeeRepository.save(employee);
  }

  @GetMapping("/employee/{id}")
  public Employee getEmployee(@PathVariable long id) {
    return employeeRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }
}
