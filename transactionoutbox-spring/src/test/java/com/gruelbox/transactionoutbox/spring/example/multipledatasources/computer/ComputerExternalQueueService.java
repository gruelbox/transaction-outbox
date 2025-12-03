package com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import org.springframework.stereotype.Service;

@Getter
@Service
public class ComputerExternalQueueService {

  private final Set<Long> attempted = new HashSet<>();
  private final List<Computer> sent = new CopyOnWriteArrayList<>();

  public void sendComputerCreatedEvent(Computer computer) {
    if (attempted.add(computer.getId())) {
      throw new RuntimeException("Temporary failure, try again");
    }
    sent.add(computer);
  }

  public void clear() {
    attempted.clear();
    sent.clear();
  }
}
