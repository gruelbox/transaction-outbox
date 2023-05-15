package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Beta
@Configuration
@Import({SpringInstantiator.class, SpringR2dbcTransactionManagerImpl.class})
public class SpringR2dbcTransactionOutboxConfiguration {}
