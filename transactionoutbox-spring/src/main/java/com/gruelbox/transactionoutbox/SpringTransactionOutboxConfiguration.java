package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Beta
@Configuration
@Import({SpringInstantiator.class, SpringTransactionManagerImpl.class})
public class SpringTransactionOutboxConfiguration {}
