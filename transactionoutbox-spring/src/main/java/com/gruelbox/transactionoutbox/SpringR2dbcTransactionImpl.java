package com.gruelbox.transactionoutbox;

import io.r2dbc.spi.Connection;
import lombok.AllArgsConstructor;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;
import reactor.util.annotation.NonNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@AllArgsConstructor
class SpringR2dbcTransactionImpl implements SpringR2dbcTransaction {

  private final Connection connection;
  private final TransactionSynchronizationManager tsm;

  @Override
  public Connection connection() {
    return connection;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Connection context() {
    return connection;
  }

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    tsm.registerSynchronization(new TransactionSynchronization() {
      @Override
      @NonNull
      public Mono<Void> afterCommit() {
        return Mono.fromFuture(hook);
      }
    });
  }
}
