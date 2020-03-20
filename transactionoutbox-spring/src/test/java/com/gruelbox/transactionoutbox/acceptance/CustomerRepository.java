package com.gruelbox.transactionoutbox.acceptance;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
interface CustomerRepository extends CrudRepository<Customer, Long> {}
