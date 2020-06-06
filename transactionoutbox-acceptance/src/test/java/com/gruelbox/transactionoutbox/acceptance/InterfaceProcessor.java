package com.gruelbox.transactionoutbox.acceptance;

import java.util.concurrent.CompletableFuture;

public interface InterfaceProcessor {

  void process(int foo, String bar);

  default CompletableFuture<Void> processAsync(int foo, String bar) {
    process(foo, bar);
    return CompletableFuture.completedFuture(null);
  }
}
