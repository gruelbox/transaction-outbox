package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Beta
@Configuration
@Import({SpringTransactionManagerImpl.class, SpringInstantiator.class})
public class SpringTransactionOutboxConfiguration {}
