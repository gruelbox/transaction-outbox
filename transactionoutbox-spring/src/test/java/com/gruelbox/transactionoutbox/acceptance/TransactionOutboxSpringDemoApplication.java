package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SpringInstantiator;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

@SpringBootApplication
public class TransactionOutboxSpringDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(TransactionOutboxSpringDemoApplication.class, args);
  }

  @Bean
  @Lazy
  public TransactionOutbox transactionOutbox(
      SpringInstantiator instantiator, SpringTransactionManager transactionManager) {
    return TransactionOutbox.builder()
        .instantiator(instantiator)
        .transactionManager(transactionManager)
        .persistor(Persistor.forDialect(Dialect.H2))
        .build();
  }
}
