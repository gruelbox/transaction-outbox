package com.gruelbox.transactionoutbox.r2dbc;

import static com.gruelbox.transactionoutbox.r2dbc.Utils.EMPTY_RESULT;

import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlResultRow;
import com.gruelbox.transactionoutbox.sql.SqlStatement;
import io.r2dbc.spi.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
class R2dbcStatement implements SqlStatement {

  private final Statement statement;
  private final R2dbcTransaction tx;
  private final Dialect dialect;
  private final int timeoutSeconds;
  private final String sql;

  R2dbcStatement(R2dbcTransaction tx, Dialect dialect, int timeoutSeconds, String sql) {
    this.tx = tx;
    this.dialect = dialect;
    this.timeoutSeconds = timeoutSeconds;
    this.sql = sql;
    this.statement = tx.connection().createStatement(sql);
  }

  @Override
  public R2dbcStatement bind(int i, Object arg) {
    log.trace("Binding {} -> {}", i, arg);
    if (arg instanceof Instant) {
      statement.bind(i, LocalDateTime.ofInstant((Instant) arg, ZoneOffset.UTC));
    } else {
      if (arg == null) {
        // Lazy, but does what we need here
        statement.bindNull(i, String.class);
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
      int expectedRowCount, Function<SqlResultRow, U> rowMapper) {
    return setQueryTimeout(timeoutSeconds)
        .then(executeQueryInternal(expectedRowCount, rowMapper))
        .toFuture();
  }

  private Mono<Integer> setQueryTimeout(int timeoutSeconds) {
    try {
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
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  private Mono<Integer> executeInternal() {
    try {
      return Mono.from(statement.execute())
          .flatMap(result -> Mono.from(result.getRowsUpdated()))
          .defaultIfEmpty(0);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  private <U> Mono<List<U>> executeQueryInternal(
      int expectedRowCount, Function<SqlResultRow, U> rowMapper) {
    try {
      return Mono.from(statement.execute())
          .map(r -> (io.r2dbc.spi.Result) r)
          .defaultIfEmpty(EMPTY_RESULT)
          .flatMapMany(result -> result.map((r, m) -> r))
          .map(
              row ->
                  dialect.mapResultFromNative(
                      new SqlResultRow() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public <V> V get(int index, Class<V> type) {
                          try {
                            if (Instant.class.equals(type)) {
                              LocalDateTime value = row.get(index, LocalDateTime.class);
                              return (V) (value == null ? null : value.toInstant(ZoneOffset.UTC));
                            } else {
                              return row.get(index, type);
                            }
                          } catch (Exception e) {
                            throw new IllegalArgumentException(
                                "Failed to fetch field [" + index + "] in " + sql, e);
                          }
                        }
                      }))
          .map(rowMapper)
          .collect(() -> new ArrayList<>(expectedRowCount), List::add);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }
}
