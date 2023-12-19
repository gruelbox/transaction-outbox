package com.gruelbox.transactionoutbox.spring;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SpringTransactionManager.class, SpringInstantiator.class})
public class SpringTransactionOutboxConfiguration {}
