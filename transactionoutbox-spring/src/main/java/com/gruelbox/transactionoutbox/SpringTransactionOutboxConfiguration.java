package com.gruelbox.transactionoutbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @deprecated use {@link
 *     com.gruelbox.transactionoutbox.spring.SpringTransactionOutboxConfiguration}.
 */
@Configuration
@Import({SpringTransactionManager.class, SpringInstantiator.class})
@Deprecated(forRemoval = true)
public class SpringTransactionOutboxConfiguration {}
