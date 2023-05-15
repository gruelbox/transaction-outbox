package com.gruelbox.transactionoutbox.spring.acceptance;

import com.gruelbox.transactionoutbox.SpringR2dbcTransaction;
import com.gruelbox.transactionoutbox.SpringTransaction;
import java.util.concurrent.CompletableFuture;

interface AsyncInterfaceProcessor {

  CompletableFuture<Void> processAsync(int foo, String bar, SpringTransaction springTransaction);
  CompletableFuture<Void> processAsync(int foo, String bar, SpringR2dbcTransaction springTransaction);
  CompletableFuture<Void> processAsync(int foo, String bar);
}
