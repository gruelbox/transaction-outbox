package com.gruelbox.transactionoutbox.acceptance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
class EventPublisher {

  @Autowired private EventRepository eventRepository;

  public void publish(Event event) {
    eventRepository.save(event);
  }
}
