package com.gruelbox.transactionoutbox.spring.demo;

import java.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
class EventPublisher {

  @Autowired private EventRepository eventRepository;

  public void publish(long id, String description, LocalDateTime time) {
    eventRepository.save(new Event(id, description, time));
  }
}
