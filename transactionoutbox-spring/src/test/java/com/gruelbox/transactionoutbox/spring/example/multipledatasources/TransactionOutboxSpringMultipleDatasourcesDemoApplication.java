package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
@EnableScheduling
public class TransactionOutboxSpringMultipleDatasourcesDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(TransactionOutboxSpringMultipleDatasourcesDemoApplication.class, args);
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
  TransactionOutbox computerTransactionOutbox(
      SpringInstantiator instantiator,
      @Qualifier("computerSpringTransactionManager") SpringTransactionManager transactionManager,
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

  @Bean
  @Lazy
  TransactionOutbox employeeTransactionOutbox(
      SpringInstantiator instantiator,
      @Qualifier("employeeSpringTransactionManager") SpringTransactionManager transactionManager,
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

  @Bean
  public SpringTransactionManager computerSpringTransactionManager(
      @Qualifier("computerTransactionManager") PlatformTransactionManager transactionManager,
      @Qualifier("computerDataSource") DataSource dataSource) {
    return new SpringTransactionManager(transactionManager, dataSource);
  }

  @Bean
  public SpringTransactionManager employeeSpringTransactionManager(
      @Qualifier("employeeTransactionManager") PlatformTransactionManager transactionManager,
      @Qualifier("employeeDataSource") DataSource dataSource) {
    return new SpringTransactionManager(transactionManager, dataSource);
  }
}
