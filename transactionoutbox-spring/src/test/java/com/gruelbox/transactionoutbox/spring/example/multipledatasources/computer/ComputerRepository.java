package com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ComputerRepository extends CrudRepository<Computer, Long> {

}
