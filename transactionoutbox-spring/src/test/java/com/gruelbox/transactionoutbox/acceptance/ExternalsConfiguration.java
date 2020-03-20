package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.SpringTransactionOutboxConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SpringTransactionOutboxConfiguration.class})
class ExternalsConfiguration {}
