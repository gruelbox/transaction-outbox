package com.gruelbox.transactionoutbox.demo;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.SpringInstantiator;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import javax.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

@SpringBootApplication
public class TransactionOutboxSpringDemoApplication {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TransactionOutboxSpringDemoApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(TransactionOutboxSpringDemoApplication.class, args);
  }

  @Bean
  @Lazy
  public TransactionOutbox transactionOutbox(
      ApplicationContext applicationContext, EntityManager entityManager) {
    return TransactionOutbox.builder()
        .dialect(Dialect.H2)
        .instantiator(SpringInstantiator.builder().applicationContext(applicationContext).build())
        .transactionManager(SpringTransactionManager.builder().entityManager(entityManager).build())
        .build();
  }
}
