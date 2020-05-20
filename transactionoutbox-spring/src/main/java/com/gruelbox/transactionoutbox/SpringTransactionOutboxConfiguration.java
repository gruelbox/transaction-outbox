package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Beta
@Configuration
@Import({
  SpringTransactionOutboxFactory.class,
  SpringTransactionManager.class,
  SpringInstantiator.class
})
public class SpringTransactionOutboxConfiguration {}
