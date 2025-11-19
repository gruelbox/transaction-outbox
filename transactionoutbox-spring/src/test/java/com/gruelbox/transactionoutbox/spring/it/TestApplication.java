package com.gruelbox.transactionoutbox.spring.it;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;
import java.time.Duration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@SpringBootApplication
@Import({SpringInstantiator.class, SpringTransactionManager.class})
public class TestApplication {

  public static void main(String[] args) {
    SpringApplication.run(TestApplication.class, args);
  }

  @Bean
  @Lazy
  Persistor persistor() {
    return Persistor.forDialect(Dialect.H2);
  }

  @Bean
  @Lazy
  TransactionOutbox transactionOutbox(
      SpringInstantiator instantiator,
      SpringTransactionManager transactionManager,
      Persistor persistor) {
    return TransactionOutbox.builder()
        .instantiator(instantiator)
        .transactionManager(transactionManager)
        .persistor(persistor)
        .attemptFrequency(Duration.ofSeconds(1))
        .blockAfterAttempts(3)
        .build();
  }
}
