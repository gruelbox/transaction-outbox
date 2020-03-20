package com.gruelbox.transactionoutbox.acceptance;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
interface EventRepository extends CrudRepository<Event, Long> {}
