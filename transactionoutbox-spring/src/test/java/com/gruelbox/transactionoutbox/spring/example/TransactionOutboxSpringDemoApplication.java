package com.gruelbox.transactionoutbox.spring.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TransactionOutboxSpringDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(TransactionOutboxSpringDemoApplication.class, args);
  }

  @Bean
  @Lazy
  Persistor persistor(TransactionOutboxProperties properties, ObjectMapper objectMapper) {
    if (properties.isUseJackson()) {
      return DefaultPersistor.builder()
          .serializer(JacksonInvocationSerializer.builder().mapper(objectMapper).build())
          .dialect(Dialect.H2)
          .build();
    } else {
      return Persistor.forDialect(Dialect.H2);
    }
  }

  @Bean
  @Lazy
  TransactionOutbox transactionOutbox(
      SpringInstantiator instantiator,
      SpringTransactionManager transactionManager,
      TransactionOutboxProperties properties,
      Persistor persistor) {
    return TransactionOutbox.builder()
        .instantiator(instantiator)
        .transactionManager(transactionManager)
        .persistor(persistor)
        .attemptFrequency(properties.getAttemptFrequency())
        .blockAfterAttempts(properties.getBlockAfterAttempts())
        .build();
  }
}
