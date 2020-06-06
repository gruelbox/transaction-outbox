package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import io.r2dbc.spi.Connection;
import java.util.concurrent.CompletableFuture;

interface InterfaceProcessor {

  void process(int foo, String bar);

  CompletableFuture<Void> processAsync(int foo, String bar, Connection connection);

  CompletableFuture<Void> processAsync(int foo, String bar, R2dbcTransaction r2dbcTransaction);
}
