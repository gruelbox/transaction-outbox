package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.spi.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

@AllArgsConstructor
final class R2dbcRawTransaction implements R2dbcTransaction<Connection> {

  private final List<Supplier<CompletableFuture<Void>>> postCommitHooks = new ArrayList<>();
  private final Connection connection;

  @Override
  public Connection connection() {
    return connection;
  }

  @Override
  public Connection context() {
    return connection;
  }

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> runnable) {
    postCommitHooks.add(runnable);
  }

  final CompletableFuture<Void> processHooks() {
    CompletableFuture[] futures =
        postCommitHooks.stream().map(Supplier::get).toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(futures);
  }
}
