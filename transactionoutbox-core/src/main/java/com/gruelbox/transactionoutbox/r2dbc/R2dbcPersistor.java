package com.gruelbox.transactionoutbox.r2dbc;

import static com.gruelbox.transactionoutbox.Utils.toRunningFuture;

import com.gruelbox.transactionoutbox.AbstractSqlPersistor;
import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * An R2DBC-based {@link Persistor} for {@link TransactionOutbox}, using purely the low-level R2DBC
 * API, so compatible with any R2DBC client API such as Spring. All operations are non-blocking.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link JdbcPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>More significant changes can be achieved by subclassing, which is explicitly supported. If, on
 * the other hand, you want to use a completely non-relational underlying data store or do something
 * equally esoteric, you may prefer to implement {@link Persistor} from the ground up.
 */
@SuperBuilder
@Slf4j
public class R2dbcPersistor extends AbstractSqlPersistor<Connection, R2dbcTransaction<?>> {

  /**
   * Uses the default relational persistor. Shortcut for: <code>
   * R2dbcPersistor.builder().dialect(dialect).build();</code>
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  public static R2dbcPersistor forDialect(Dialect dialect) {
    return R2dbcPersistor.builder().dialect(dialect).build();
  }

  @Override
  public CompletableFuture<Void> migrate(
      TransactionManager<Connection, ?, ? extends R2dbcTransaction<?>> transactionManager) {
    return R2dbcMigrationManager.migrate(transactionManager);
  }

  @Override
  public CompletableFuture<Void> save(
      R2dbcTransaction<?> r2dbcTransaction, TransactionOutboxEntry entry) {

    // TODO remove hack https://github.com/mirromutth/r2dbc-mysql/issues/111
    if (dialect.equals(Dialect.MY_SQL_5) || dialect.equals(Dialect.MY_SQL_8)) {
      entry.setNextAttemptTime(entry.getNextAttemptTime().truncatedTo(ChronoUnit.SECONDS));
    }

    return super.save(r2dbcTransaction, entry)
        .exceptionally(
            t -> {
              try {
                if (t instanceof CompletionException) {
                  throw t.getCause();
                } else {
                  throw t;
                }
              } catch (R2dbcDataIntegrityViolationException e) {
                throw new AlreadyScheduledException(
                    "Request " + entry.description() + " already exists", e);
              } catch (Throwable e) {
                throw (RuntimeException) Utils.uncheckAndThrow(e);
              }
            });
  }

  @Override
  protected String preParse(String sql) {
    if (!dialect.equals(Dialect.POSTGRESQL_9)) {
      return super.preParse(sql);
    }
    // Blunt, not general purpose, but only needs to work for the known use cases
    StringBuilder builder = new StringBuilder();
    int paramIndex = 1;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '?') {
        builder.append('$').append(paramIndex++);
      } else {
        builder.append(c);
      }
    }
    return builder.toString();
  }

  @Override
  protected <T> T statement(
      R2dbcTransaction<?> tx, String sql, boolean batchable, Function<Binder, T> binding) {
    var statement = tx.connection().createStatement(sql);
    return binding.apply(
        new Binder() {

          @Override
          public Binder bind(int i, Object arg) {
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
          public CompletableFuture<Result> execute() {
            return toRunningFuture(statement.execute())
                .thenApply(result -> () -> toRunningFuture(result.getRowsUpdated()));
          }

          @Override
          @SuppressWarnings({"unchecked", "ConstantConditions"})
          public <T> CompletableFuture<List<T>> executeQuery(
              int expectedRowCount, Function<ResultRow, T> rowMapper) {
            return Mono.from(statement.execute())
                .flatMapMany(result -> result.map(RowAndMeta::new))
                .map(
                    row ->
                        new ResultRow() {
                          @Override
                          public <U> U get(int index, Class<U> type) {
                            try {
                              // TODO suggest Instant support to R2DBC
                              if (Instant.class.equals(type)) {
                                return (U)
                                    Objects.requireNonNull(
                                            row.getRow().get(index, LocalDateTime.class))
                                        .toInstant(ZoneOffset.UTC);
                                // TODO remove hack regarding data types
                              } else if (Boolean.class.equals(type)
                                  && (dialect.equals(Dialect.MY_SQL_5)
                                      || dialect.equals(Dialect.MY_SQL_8))) {
                                return (U)
                                    Boolean.valueOf(row.getRow().get(index, Short.class) == 1);
                              } else {
                                return row.getRow().get(index, type);
                              }
                            } catch (Exception e) {
                              throw new IllegalArgumentException(
                                  "Failed to fetch field ["
                                      + index
                                      + "] from row "
                                      + row.getMeta()
                                      + "in "
                                      + sql,
                                  e);
                            }
                          }
                        })
                .map(rowMapper)
                .collect(
                    () -> new ArrayList<>(expectedRowCount), (BiConsumer<List<T>, T>) List::add)
                .toFuture();
          }
        });
  }

  @Value
  private static class RowAndMeta {
    Row row;
    RowMetadata meta;
  }
}
