package com.gruelbox.transactionoutbox.r2dbc;

import static com.gruelbox.transactionoutbox.r2dbc.Utils.EMPTY_RESULT;

import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.DialectFamily;
import com.gruelbox.transactionoutbox.sql.SqlPersistor.Binder;
import com.gruelbox.transactionoutbox.sql.SqlPersistor.ResultRow;
import io.r2dbc.spi.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
class R2dbcStatement implements Binder {

  private final Statement statement;
  private final R2dbcTransaction<?> tx;
  private final Dialect dialect;
  private final int timeoutSeconds;
  private final String sql;

  R2dbcStatement(R2dbcTransaction<?> tx, Dialect dialect, int timeoutSeconds, String sql) {
    this.tx = tx;
    this.dialect = dialect;
    this.timeoutSeconds = timeoutSeconds;
    this.sql = sql;
    this.statement = tx.connection().createStatement(sql);
  }

  @Override
  public R2dbcStatement bind(int i, Object arg) {
    log.trace("Binding {} -> {}", i, arg);
    // TODO suggest Instant support to R2DBC
    if (arg instanceof Instant) {
      statement.bind(i, LocalDateTime.ofInstant((Instant) arg, ZoneOffset.UTC));
    } else {
      if (arg == null) {
        // TODO highlight this as a problem with the R2DBC API
        statement.bindNull(i, String.class); // Lazy, but does what we need here
      } else {
        statement.bind(i, arg);
      }
    }
    return this;
  }

  @Override
  public CompletableFuture<Integer> execute() {
    return setQueryTimeout(timeoutSeconds).then(executeInternal()).toFuture();
  }

  @Override
  public <U> CompletableFuture<List<U>> executeQuery(
      int expectedRowCount, Function<ResultRow, U> rowMapper) {
    return setQueryTimeout(timeoutSeconds)
        .then(executeQueryInternal(expectedRowCount, rowMapper))
        .toFuture();
  }

  private Mono<Integer> setQueryTimeout(int timeoutSeconds) {
    if (timeoutSeconds <= 0) {
      return Mono.empty();
    }
    var queryTimeoutSetup = dialect.getQueryTimeoutSetup();
    if (queryTimeoutSetup == null || queryTimeoutSetup.isEmpty()) {
      log.warn("Dialect {} not set up with query timeout support", dialect);
      return Mono.empty();
    }
    var sql = queryTimeoutSetup.replace("{{timeout}}", Integer.toString(timeoutSeconds));
    if (sql.equals(queryTimeoutSetup)) {
      return new R2dbcStatement(tx, dialect, 0, sql).bind(0, timeoutSeconds).executeInternal();
    } else {
      return new R2dbcStatement(tx, dialect, 0, sql).executeInternal();
    }
  }

  private Mono<Integer> executeInternal() {
    return Mono.from(statement.execute())
        .flatMap(result -> Mono.from(result.getRowsUpdated()))
        .defaultIfEmpty(0);
  }

  private <U> Mono<List<U>> executeQueryInternal(
      int expectedRowCount, Function<ResultRow, U> rowMapper) {
    return Mono.from(statement.execute())
        .map(r -> (io.r2dbc.spi.Result) r)
        .defaultIfEmpty(EMPTY_RESULT)
        .flatMapMany(result -> result.map((r, m) -> r))
        .map(
            row ->
                new ResultRow() {
                  @SuppressWarnings({"unchecked", "ConstantConditions"})
                  @Override
                  public <V> V get(int index, Class<V> type) {
                    try {
                      // TODO suggest Instant support to R2DBC
                      if (Instant.class.equals(type)) {
                        return (V)
                            Objects.requireNonNull(row.get(index, LocalDateTime.class))
                                .toInstant(ZoneOffset.UTC);
                        // TODO remove hack regarding data types
                      } else if (Boolean.class.equals(type)
                          && (dialect.getFamily().equals(DialectFamily.MY_SQL))) {
                        return (V) Boolean.valueOf(row.get(index, Short.class) == 1);
                      } else {
                        return row.get(index, type);
                      }
                    } catch (Exception e) {
                      throw new IllegalArgumentException(
                          "Failed to fetch field [" + index + "] in " + sql, e);
                    }
                  }
                })
        .map(rowMapper)
        .collect(() -> new ArrayList<>(expectedRowCount), List::add);
  }
}
