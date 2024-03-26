package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import com.gruelbox.transactionoutbox.spring.SpringTransactionManager;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SpringInstantiator.class, SpringTransactionManager.class})
class ExternalsConfiguration {}
