package com.gruelbox.transactionoutbox.spring.example.multipledatasources;

import com.gruelbox.transactionoutbox.spring.SpringInstantiator;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SpringInstantiator.class})
class ExternalsConfiguration {}
