package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.DefaultInvocationSerializer;
import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.ExecutorSubmitter;
import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.sql.Connection;
import java.time.Duration;
import java.util.Currency;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import javax.sql.DataSource;
import lombok.Value;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.slf4j.event.Level;

/**
 * Just syntax-checks the example given in the README to give a warning if the example needs to
 * change.
 */
class TestComplexConfigurationExample {

  @Test
  @Disabled
  void test() {

    DataSource dataSource = Mockito.mock(DataSource.class);
    ServiceLocator myServiceLocator = Mockito.mock(ServiceLocator.class);
    Executor executor = ForkJoinPool.commonPool();
    EventPublisher eventPublisher = Mockito.mock(EventPublisher.class);

    TransactionManager transactionManager = TransactionManager.fromDataSource(dataSource);

    TransactionOutbox outbox =
        TransactionOutbox.builder()
            // The most complex part to set up for most will be synchronizing with your existing
            // transaction
            // management. Pre-rolled implementations are available for jOOQ and Spring (see above
            // for more information)
            // and you can use those examples to synchronize with anything else by defining your own
            // TransactionManager.
            // Or, if you have no formal transaction management at the moment, why not start, using
            // transaction-outbox's
            // built-in one?
            .transactionManager(transactionManager)
            // Modify how requests are persisted to the database.
            .persistor(
                DefaultPersistor.builder()
                    // Selecting the right SQL dialect ensures that features such as SKIP LOCKED are
                    // used correctly.
                    .dialect(Dialect.POSTGRESQL_9)
                    // Override the table name (defaults to "TXNO_OUTBOX")
                    .tableName("transactionOutbox")
                    // Shorten the time we will wait for write locks (defaults to 2)
                    .writeLockTimeoutSeconds(1)
                    // Disable automatic creation and migration of the outbox table, forcing the
                    // application to manage
                    // migrations itself
                    .migrate(false)
                    // Allow the SaleType enum and Money class to be used in arguments (see example
                    // below)
                    .serializer(
                        DefaultInvocationSerializer.builder()
                            .serializableTypes(Set.of(SaleType.class, Money.class))
                            .build())
                    .build())
            .instantiator(Instantiator.using(myServiceLocator::createInstance))
            // Change the log level used when work cannot be submitted to a saturated queue to INFO
            // level (the default
            // is WARN, which you should probably consider a production incident). You can also
            // change the Executor used
            // for submitting work to a shared thread pool used by the rest of your application.
            // Fully-custom Submitter
            // implementations are also easy to implement.
            .submitter(
                ExecutorSubmitter.builder()
                    .executor(ForkJoinPool.commonPool())
                    .logLevelWorkQueueSaturation(Level.INFO)
                    .build())
            // Lower the log level when a task fails temporarily from the default WARN.
            .logLevelTemporaryFailure(Level.INFO)
            // 10 attempts at a task before it is blocked (and would require intervention)
            .blockAfterAttempts(10)
            // When calling flush(), select 0.5m records at a time.
            .flushBatchSize(500_000)
            // Flush once every 15 minutes only
            .attemptFrequency(Duration.ofMinutes(15))
            // Include Slf4j's Mapped Diagnostic Context in tasks. This means that anything in the
            // MDC when schedule()
            // is called will be recreated in the task when it runs. Very useful for tracking things
            // like user ids and
            // request ids across invocations.
            .serializeMdc(true)
            // We can intercept task successes, single failures and blocked tasks. The most common
            // use is
            // to catch blocked tasks.
            // and raise alerts for these to be investigated. A Slack interactive message is
            // particularly effective here
            // since it can be wired up to call unblock() automatically.
            .listener(
                new TransactionOutboxListener() {

                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    eventPublisher.publish(new OutboxTaskProcessedEvent(entry.getId()));
                  }

                  @Override
                  public void blocked(TransactionOutboxEntry entry, Throwable cause) {
                    eventPublisher.publish(new BlockedOutboxTaskEvent(entry.getId()));
                  }
                })
            .build();

    // Usage example, using the in-built transaction manager
    MDC.put("SESSIONKEY", "Foo");
    try {
      transactionManager.inTransaction(
          tx -> {
            writeSomeChanges(tx.connection());
            outbox
                .schedule(getClass())
                .performRemoteCall(SaleType.SALE, Money.of(10, Currency.getInstance("USD")));
          });
    } finally {
      MDC.clear();
    }
  }

  void performRemoteCall(SaleType saleType, Money amount) {}

  private void writeSomeChanges(Connection connection) {}

  private interface ServiceLocator {
    <T> T createInstance(Class<T> clazz);
  }

  private interface EventPublisher {
    void publish(Object o);
  }

  @Value
  private static class BlockedOutboxTaskEvent {
    String id;
  }

  @Value
  private static class OutboxTaskProcessedEvent {
    String id;
  }

  private enum SaleType {
    SALE,
    REFUND
  }

  private interface Money {
    static Money of(int amount, Currency currency) {
      return null;
    }
  }
}
