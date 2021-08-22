package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Beta
public interface SqlStatement {

  SqlStatement bindNull(int index, Class<?> clazz);

  SqlStatement bind(int index, Object value);

  default <T> SqlStatement bind(int index, T obj, Class<T> clazz) {
    if (obj == null) {
      return bindNull(index, clazz);
    } else {
      return bind(index, obj);
    }
  }

  CompletableFuture<Integer> execute();

  <T> CompletableFuture<List<T>> executeQuery(
      int expectedRowCount, Function<SqlResultRow, T> rowMapper);
}
