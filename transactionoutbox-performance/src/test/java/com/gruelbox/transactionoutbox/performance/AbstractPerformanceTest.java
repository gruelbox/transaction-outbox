package com.gruelbox.transactionoutbox.performance;

import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public abstract class AbstractPerformanceTest {
  protected abstract Dialect dialect();

  protected Persistor persistor = DefaultPersistor.builder().dialect(dialect()).build();

  Persistor persistor() {
    return persistor;
  }

  protected abstract TransactionManager txManager();

  @Test
  public void testInsertAndSelect() {
    // Given
    TransactionOutbox outbox =
        TransactionOutbox.builder().transactionManager(txManager()).persistor(persistor()).build();

    // When
    txManager()
        .inTransaction(tx -> outbox.schedule(Widget.class).widgetize(UUID.randomUUID().toString()));
  }
}
