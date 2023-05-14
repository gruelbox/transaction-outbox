package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.uncheck;

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionalInvocation;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class SimpleTransactionManagerImpl
    extends AbstractThreadLocalJdbcTransactionManager<SimpleTransaction<Void>>
    implements InitializationEventPublisher, SimpleTransactionManager {

  private final JdbcConnectionProvider connectionProvider;

  private SimpleTransactionManagerImpl(JdbcConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(JdbcTransaction.class));
    eventBus.sendEvent(
        SerializableTypeRequired.class, new SerializableTypeRequired(SimpleTransaction.class));
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    return withTransaction(
        atx -> {
          T result = processAndCommitOrRollback(work, atx);
          atx.processHooks();
          return result;
        });
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocationOptional(
            method, args, Void.class, ctx -> null)
        .orElse(super.extractTransaction(method, args));
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, SimpleTransaction<Void> transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, Void.class, transaction);
  }

  private <T, E extends Exception> T processAndCommitOrRollback(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work,
      SimpleTransaction<Void> transaction)
      throws E {
    try {
      log.debug("Processing work");
      T result = work.doWork(transaction);
      transaction.flushBatches();
      log.debug("Committing transaction");
      transaction.commit();
      return result;
    } catch (Exception e) {
      try {
        log.warn(
            "Exception in transactional block ({}{}). Rolling back. See later messages for detail",
            e.getClass().getSimpleName(),
            e.getMessage() == null ? "" : (" - " + e.getMessage()));
        transaction.rollback();
      } catch (Exception ex) {
        log.warn("Failed to roll back", ex);
      }
      throw e;
    }
  }

  private <T, E extends Exception> T withTransaction(
      ThrowingTransactionalSupplier<T, E, SimpleTransaction<Void>> work) throws E {
    try (var connection = connectionProvider.obtainConnection()) {
      try (var transaction = pushTransaction(new SimpleTransaction<>(connection, null))) {
        log.debug("Got connection {}", connection);
        boolean autoCommit = transaction.connection().getAutoCommit();
        if (autoCommit) {
          log.debug("Setting auto-commit false");
          uncheck(() -> transaction.connection().setAutoCommit(false));
        }
        try {
          return work.doWork(transaction);
        } finally {
          connection.setAutoCommit(autoCommit);
        }
      } finally {
        popTransaction();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static final class Builder implements SimpleTransactionManager.SimpleTransactionManagerBuilder {
    private JdbcConnectionProvider connectionProvider;

    Builder() {}

    public Builder connectionProvider(JdbcConnectionProvider connectionProvider) {
      this.connectionProvider = connectionProvider;
      return this;
    }

    public SimpleTransactionManagerImpl build() {
      return new SimpleTransactionManagerImpl(connectionProvider);
    }

    public String toString() {
      return "SimpleTransactionManagerImpl.SimpleTransactionManagerImplBuilder(connectionProvider="
          + this.connectionProvider
          + ")";
    }
  }
}
