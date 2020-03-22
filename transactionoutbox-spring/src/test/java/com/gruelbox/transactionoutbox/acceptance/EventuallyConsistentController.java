package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.time.LocalDateTime;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class EventuallyConsistentController {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EventuallyConsistentController.class);

  @Autowired private CustomerRepository customerRepository;
  @Autowired private Provider<TransactionOutbox> outbox;
  @Autowired private EventRepository eventRepository;
  @Autowired private EventPublisher eventPublisher;

  @SuppressWarnings("SameReturnValue")
  @RequestMapping("/createCustomer")
  @Transactional
  public String createCustomer() {
    LOGGER.info("Creating customers");
    outbox
        .get()
        .schedule(eventPublisher.getClass()) // Just a trick to get autowiring to work.
        .publish(1L, "Created customers", LocalDateTime.now());
    customerRepository.save(new Customer(1L, "Martin", "Carthy"));
    customerRepository.save(new Customer(2L, "Dave", "Pegg"));
    LOGGER.info("Customers created");
    return "Done";
  }

  @RequestMapping("/gotEventAndCustomers")
  public String gotEvent() {
    var event = eventRepository.findById(1L);
    var customer1 = customerRepository.findById(1L);
    var customer2 = customerRepository.findById(2L);
    return event.isPresent() && customer1.isPresent() && customer2.isPresent() ? "Yes" : "No";
  }
}
