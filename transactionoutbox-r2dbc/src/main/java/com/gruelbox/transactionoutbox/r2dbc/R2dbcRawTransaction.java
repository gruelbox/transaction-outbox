package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.spi.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public final class R2dbcRawTransaction implements R2dbcTransaction {

  private final List<Supplier<CompletableFuture<Void>>> postCommitHooks = new ArrayList<>();
  private final Connection connection;

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
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> runnable) {
    postCommitHooks.add(runnable);
  }

  final CompletableFuture<Void> processHooks() {
    log.debug("{} hooks to process", postCommitHooks.size());
    CompletableFuture[] futures =
        postCommitHooks.stream()
            .map(Supplier::get)
            .map(cf -> cf.thenRun(() -> log.debug("Processed post commit hook")))
            .toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(futures);
  }
}
