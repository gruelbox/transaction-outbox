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
    statement.bind(i, mapObj(arg));
    return this;
  }

  @Override
  public SqlStatement bindNull(int i, Class<?> clazz) {
    log.trace("Binding {} -> null", i);
    statement.bindNull(i, mapClass(clazz));
    return this;
  }

  private Class<?> mapClass(Class<?> clazz) {
    if (clazz.equals(Instant.class)) {
      return LocalDateTime.class;
    } else {
      return clazz;
    }
  }

  private Object mapObj(Object o) {
    if (o instanceof Instant) {
      return LocalDateTime.ofInstant((Instant) o, ZoneOffset.UTC);
    } else {
      return o;
    }
  }

  @Override
  public CompletableFuture<Long> execute() {
    return setQueryTimeout(timeoutSeconds).then(executeInternal()).toFuture();
  }

  @Override
  public <U> CompletableFuture<List<U>> executeQuery(
      int expectedRowCount, Function<SqlResultRow, U> rowMapper) {
    return setQueryTimeout(timeoutSeconds)
        .then(executeQueryInternal(expectedRowCount, rowMapper))
        .toFuture();
  }

  private Mono<Long> setQueryTimeout(int timeoutSeconds) {
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

  private Mono<Long> executeInternal() {
    try {
      return Mono.from(statement.execute())
          .flatMap(result -> Mono.from(result.getRowsUpdated()))
          .defaultIfEmpty(0L);
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
          .flatMapMany(result -> result.map((r, m) -> mapSpiRow(rowMapper, r)))
          .collect(() -> new ArrayList<>(expectedRowCount), List::add);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  private <U> U mapSpiRow(Function<SqlResultRow, U> rowMapper, io.r2dbc.spi.Row r) {
    return rowMapper.apply(spiRowToSqlResultRow(r));
  }

  private SqlResultRow spiRowToSqlResultRow(io.r2dbc.spi.Row row) {
    return dialect.mapResultFromNative(
        new SqlResultRow() {
          @SuppressWarnings("unchecked")
          @Override
          public <V> V get(int index, Class<V> type) {
            try {
              if (Instant.class.equals(type)) {
                LocalDateTime value = row.get(index, LocalDateTime.class);
                return (V) (value == null ? null : value.toInstant(ZoneOffset.UTC));
              } else if (Integer.class.equals(type)) {
                var value = row.get(index, Number.class);
                return (V) (value == null ? null : value.intValue());
              } else if (Long.class.equals(type)) {
                var value = row.get(index, Number.class);
                return (V) (value == null ? null : value.longValue());
              } else {
                return row.get(index, type);
              }
            } catch (Exception e) {
              throw new IllegalArgumentException(
                  "Failed to fetch field [" + index + "] in " + sql, e);
            }
          }
        });
  }
}
