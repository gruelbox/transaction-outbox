package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Beta
@Configuration
@Import(SpringInstantiator.class)
public class SpringTransactionOutboxConfiguration {

  @Bean
  @Lazy
  SpringTransactionManager txManager() {
    return new SpringTransactionManagerImpl();
  }

}
