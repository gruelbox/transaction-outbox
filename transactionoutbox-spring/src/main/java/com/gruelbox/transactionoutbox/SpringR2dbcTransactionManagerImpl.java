package com.gruelbox.transactionoutbox;

import static reactor.core.publisher.Mono.fromCompletionStage;

import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.r2dbc.connection.ConnectionHolder;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Beta
@Service
class SpringR2dbcTransactionManagerImpl implements SpringR2dbcTransactionManager {

  private final ConnectionFactory cf;
  private final TransactionalOperator rxtx;
  private final TransactionSynchronizationManager tsm;

  SpringR2dbcTransactionManagerImpl(ConnectionFactory cf, TransactionSynchronizationManager tsm) {
    this.cf = cf;
    this.tsm = tsm;
    this.rxtx = TransactionalOperator.create(new R2dbcTransactionManager(cf));
  }

  @Override
  public void onPublishInitializationEvents(InitializationEventBus eventBus) {
    eventBus.sendEvent(
      SerializableTypeRequired.class, new SerializableTypeRequired(R2dbcTransaction.class));
    eventBus.sendEvent(
      SerializableTypeRequired.class, new SerializableTypeRequired(SpringR2dbcTransaction.class));
    eventBus.sendEvent(
      SerializableTypeRequired.class, new SerializableTypeRequired(Connection.class));
  }

  @Override
  public <T> CompletableFuture<T> transactionally(Function<SpringR2dbcTransaction, CompletableFuture<T>> fn) {
    return rxtx.transactional(
        fromCompletionStage(fn.apply(getCurrentTx()).thenApply(Optional::ofNullable)))
          .toFuture()
      .thenApply(opt -> opt.orElse(null));
  }

  @Override
  public TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.toTransactionalInvocation(method, args, getCurrentTx());
  }

  @Override
  public Invocation injectTransaction(Invocation invocation, SpringR2dbcTransaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(invocation, Void.class, transaction);
  }

  private SpringR2dbcTransactionImpl getCurrentTx() {
    return new SpringR2dbcTransactionImpl(getCurrentConnection(), tsm);
  }

  private Connection getCurrentConnection() {
    return ((ConnectionHolder) Objects.requireNonNull(tsm.getResource(cf))).getConnection();
  }
}
