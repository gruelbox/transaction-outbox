package com.gruelbox.transactionoutbox.spring.example;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
interface CustomerRepository extends CrudRepository<Customer, Long> {}
