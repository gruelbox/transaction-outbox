package com.gruelbox.transactionoutbox.demo;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class EventuallyConsistentController {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EventuallyConsistentController.class);

  @Autowired private CustomerRepository customerRepository;

  @Autowired private EventRepository eventRepository;

  @Autowired @Lazy private TransactionOutbox outbox;

  @SuppressWarnings("SameReturnValue")
  @RequestMapping("/createCustomer")
  @Transactional
  public String createCustomer() {
    LOGGER.info("Creating customers");
    outbox
        .schedule(EventRepository.class)
        .save(new Event(1L, "Created customers", LocalDateTime.now()));
    customerRepository.save(new Customer("Martin", "Carthy"));
    customerRepository.save(new Customer("Dave", "Pegg"));
    LOGGER.info("Customers created");
    return "Done";
  }

  @RequestMapping("/gotEvent")
  public String gotEvent() {
    return eventRepository.findById(1L).isPresent() ? "No" : "Yes";
  }
}
