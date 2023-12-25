package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@SuppressWarnings("unused")
@RestController
class EventuallyConsistentController {

  @Autowired private CustomerRepository customerRepository;
  @Autowired private TransactionOutbox outbox;

  @SuppressWarnings("SameReturnValue")
  @PostMapping(path = "/customer")
  @Transactional
  public void createCustomer(@RequestBody Customer customer, @RequestParam(name = "ordered", required = false) Boolean ordered) {
    customerRepository.save(customer);
    if (ordered != null && ordered) {
      outbox.with().ordered("justonetopic").schedule(ExternalQueueService.class).sendCustomerCreatedEvent(customer);
    } else {
      outbox.schedule(ExternalQueueService.class).sendCustomerCreatedEvent(customer);
    }
  }

  @GetMapping("/customer/{id}")
  public Customer getCustomer(@PathVariable long id) {
    return customerRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }
}
