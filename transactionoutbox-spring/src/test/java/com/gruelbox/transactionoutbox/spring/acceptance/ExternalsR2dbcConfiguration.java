package com.gruelbox.transactionoutbox.spring.acceptance;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SpringInstantiator;
import com.gruelbox.transactionoutbox.SpringR2dbcTransactionManager;
import com.gruelbox.transactionoutbox.SpringR2dbcTransactionOutboxConfiguration;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.SpringTransactionOutboxConfiguration;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.sql.Dialects;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@Configuration
@Import({SpringR2dbcTransactionOutboxConfiguration.class})
class ExternalsR2dbcConfiguration {

  @Bean
  @Lazy
  public TransactionOutbox transactionOutbox(
      SpringInstantiator instantiator, SpringR2dbcTransactionManager transactionManager) {
    return TransactionOutbox.builder()
        .instantiator(instantiator)
        .transactionManager(transactionManager)
        .persistor(Persistor.forDialect(Dialects.H2))
        .build();
  }
}
