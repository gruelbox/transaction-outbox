package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

interface AsyncInterfaceProcessor {

  void process(int foo, String bar);

  CompletableFuture<Void> processAsync(int foo, String bar, Connection connection);

  CompletableFuture<Void> processAsync(int foo, String bar, JdbcTransaction r2dbcTransaction);
}
