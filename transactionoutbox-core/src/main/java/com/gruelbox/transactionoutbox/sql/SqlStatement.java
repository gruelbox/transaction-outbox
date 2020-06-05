package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Beta
public interface SqlStatement {

  SqlStatement bind(int index, Object value);

  CompletableFuture<Integer> execute();

  <T> CompletableFuture<List<T>> executeQuery(
      int expectedRowCount, Function<SqlResultRow, T> rowMapper);
}
