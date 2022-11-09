package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.SpringTransactionOutboxConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SpringTransactionOutboxConfiguration.class})
class ExternalsConfiguration {
}
