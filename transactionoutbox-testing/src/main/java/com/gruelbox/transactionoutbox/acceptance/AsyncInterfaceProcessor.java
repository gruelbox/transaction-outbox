package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.util.concurrent.CompletableFuture;

interface AsyncInterfaceProcessor {

  CompletableFuture<Void> processAsync(int foo, String bar, JdbcTransaction r2dbcTransaction);
}
