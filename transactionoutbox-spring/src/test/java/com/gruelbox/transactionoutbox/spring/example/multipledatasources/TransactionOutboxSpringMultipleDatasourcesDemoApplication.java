package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TransactionOutboxSpringMultipleDatasourcesDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(TransactionOutboxSpringMultipleDatasourcesDemoApplication.class, args);
  }
}
