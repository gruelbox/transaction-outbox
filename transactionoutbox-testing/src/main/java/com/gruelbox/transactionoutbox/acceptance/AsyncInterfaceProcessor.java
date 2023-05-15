package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import io.r2dbc.spi.Connection;

import java.util.concurrent.CompletableFuture;

interface AsyncInterfaceProcessor {

  CompletableFuture<Void> processAsync(int foo, String bar, JdbcTransaction jdbcTransaction);
  CompletableFuture<Void> processAsync(int foo, String bar, R2dbcTransaction r2dbcTransaction);
  CompletableFuture<Void> processAsync(int foo, String bar, Connection connection);

}
