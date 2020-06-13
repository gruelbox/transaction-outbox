package com.gruelbox.transactionoutbox.spring.demo;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
interface EventRepository extends CrudRepository<Event, Long> {}
