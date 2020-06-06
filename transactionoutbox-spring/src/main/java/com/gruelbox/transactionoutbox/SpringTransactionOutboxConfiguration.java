package com.gruelbox.transactionoutbox;

import javax.persistence.EntityManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Beta
@Configuration
@Import({ SpringInstantiator.class, SpringTransactionEntryPoints.class })
public class SpringTransactionOutboxConfiguration {

  @Bean
  @Lazy
  SpringTransactionManager txManager(EntityManager entityManager, SpringTransactionEntryPoints entryPoints) {
    return new SpringTransactionManagerImpl(entityManager, entryPoints);
  }

}
