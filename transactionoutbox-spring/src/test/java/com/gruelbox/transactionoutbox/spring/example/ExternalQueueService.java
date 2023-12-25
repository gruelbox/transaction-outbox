package com.gruelbox.transactionoutbox.spring.example;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

@Getter
@Service
class ExternalQueueService {

  private final Set<Long> attempted = new HashSet<>();
  private final List<Customer> sent = new CopyOnWriteArrayList<>();

  void sendCustomerCreatedEvent(Customer customer) {
    if (attempted.add(customer.getId())) {
      throw new RuntimeException("Temporary failure, try again");
    }
    sent.add(customer);
  }

  public void clear() {
    attempted.clear();
    sent.clear();
  }
}
