package com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import org.springframework.stereotype.Service;

@Getter
@Service
public class EmployeeExternalQueueService {

  private final Set<Long> attempted = new HashSet<>();
  private final List<Employee> sent = new CopyOnWriteArrayList<>();

  public void sendEmployeeCreatedEvent(Employee employee) {
    if (attempted.add(employee.getId())) {
      throw new RuntimeException("Temporary failure, try again");
    }
    sent.add(employee);
  }

  public void clear() {
    attempted.clear();
    sent.clear();
  }
}
