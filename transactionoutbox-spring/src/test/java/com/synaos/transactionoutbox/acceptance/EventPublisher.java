package com.synaos.transactionoutbox.acceptance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
class EventPublisher {

    @Autowired
    private EventRepository eventRepository;

    public void publish(long id, String description, LocalDateTime time) {
        eventRepository.save(new Event(id, description, time));
    }
}
