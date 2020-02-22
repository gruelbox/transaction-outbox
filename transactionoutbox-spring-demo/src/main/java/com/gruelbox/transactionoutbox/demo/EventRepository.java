package com.gruelbox.transactionoutbox.demo;

import org.springframework.data.repository.CrudRepository;

interface EventRepository extends CrudRepository<Event, Long> {}
