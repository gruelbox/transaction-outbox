package com.gruelbox.transactionoutbox.spring;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @deprecated Just {@code @Import} the components you need.
 */
@Configuration
@Deprecated(forRemoval = true)
@Import({SpringTransactionManager.class, SpringInstantiator.class})
public class SpringTransactionOutboxConfiguration {}
