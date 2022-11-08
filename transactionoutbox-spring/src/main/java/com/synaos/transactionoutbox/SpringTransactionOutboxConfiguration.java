package com.synaos.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Beta
@Configuration
@Import({SpringTransactionManager.class, SpringInstantiator.class})
public class SpringTransactionOutboxConfiguration {

}
