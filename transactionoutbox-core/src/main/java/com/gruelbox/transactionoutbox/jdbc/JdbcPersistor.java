package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.AbstractSqlPersistor;
import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * The default JDBC-based {@link Persistor} for {@link TransactionOutbox}.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link JdbcPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>All operations are blocking, despite returning {@link CompletableFuture}s. No attempt is made
 * to farm off the I/O to additional threads, which would be unlikely to work with JDBC {@link
 * java.sql.Connection}s. As a result, all methods should simply be called followed immediately with
 * {@link CompletableFuture#get()} to obtain the results.
 *
 * <p>More significant changes can be achieved by subclassing, which is explicitly supported. If, on
 * the other hand, you want to use a completely non-relational underlying data store or do something
 * equally esoteric, you may prefer to implement {@link Persistor} from the ground up.
 */
@Slf4j
@SuperBuilder
public class JdbcPersistor extends AbstractSqlPersistor<Connection, JdbcTransaction<?>> {

  /**
   * Uses the default relational persistor. Shortcut for: <code>
   * JdbcPersistor.builder().dialect(dialect).build();</code>
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  public static JdbcPersistor forDialect(Dialect dialect) {
    return JdbcPersistor.builder().dialect(dialect).build();
  }

  @Override
  public CompletableFuture<Void> migrate(
      TransactionManager<Connection, ?, ? extends JdbcTransaction<?>> transactionManager) {
    return toBlockingFuture(
        () -> JdbcMigrationManager.migrate((JdbcTransactionManager) transactionManager));
  }

  @Override
  public CompletableFuture<Void> save(
      JdbcTransaction<?> jdbcTransaction, TransactionOutboxEntry entry) {
    return super.save(jdbcTransaction, entry)
        .exceptionally(
            t -> {
              try {
                if (t instanceof CompletionException) {
                  throw t.getCause();
                } else {
                  throw t;
                }
              } catch (SQLIntegrityConstraintViolationException e) {
                throw new AlreadyScheduledException(
                    "Request " + entry.description() + " already exists", e);
              } catch (Throwable e) {
                if (e.getClass().getName().equals("org.postgresql.util.PSQLException")
                    && e.getMessage().contains("constraint")) {
                  throw new AlreadyScheduledException(
                      "Request " + entry.description() + " already exists", e.getCause());
                }
                throw (RuntimeException) Utils.uncheckAndThrow(e);
              }
            });
  }

  @Override
  protected <T> T statement(
      JdbcTransaction<?> tx, String sql, boolean batchable, Function<Binder, T> binding) {
    log.debug("Executing: {}", sql);
    if (batchable) {
      return executeWithSharedStatement(tx, sql, binding);
    } else {
      return executeWithStandaloneStatement(tx, sql, binding);
    }
  }

  private <T> T executeWithStandaloneStatement(
      JdbcTransaction<?> tx, String sql, Function<Binder, T> binding) {
    try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
      return binding.apply(executeAndMap(statement));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T executeWithSharedStatement(
      JdbcTransaction<?> tx, String sql, Function<Binder, T> binding) {
    PreparedStatement statement = tx.prepareBatchStatement(sql);
    return binding.apply(executeAndMap(statement));
  }

  private Binder executeAndMap(PreparedStatement statement) {
    return new Binder() {

      @Override
      public Binder bind(int index, Object value) {
        try {
          log.debug(" - Binding {} --> {}", index, value);
          if (value instanceof Instant) {
            statement.setObject(index + 1, Timestamp.from((Instant) value));
          } else {
            statement.setObject(index + 1, value);
          }
          return this;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public CompletableFuture<Result> execute() {
        return toBlockingFuture(
            () -> {
              int rowsUpdated = statement.executeUpdate();
              return () -> completedFuture(rowsUpdated);
            });
      }

      @Override
      public <T> CompletableFuture<List<T>> executeQuery(
          int expectedResultSize, Function<ResultRow, T> rowMapper) {
        return toBlockingFuture(
            () -> {
              List<T> results = new ArrayList<>(expectedResultSize);
              try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                  log.debug("Fetched row");
                  results.add(
                      rowMapper.apply(
                          new ResultRow() {
                            @SuppressWarnings({"unchecked", "WrapperTypeMayBePrimitive"})
                            @Override
                            public <T> T get(int index, Class<T> type) {
                              try {
                                if (String.class.equals(type)) {
                                  return (T) rs.getString(index + 1);
                                } else if (Integer.class.equals(type)) {
                                  Integer value = rs.getInt(index + 1);
                                  checkNull(index + 1, rs);
                                  return (T) value;
                                } else if (Boolean.class.equals(type)) {
                                  Boolean value = rs.getBoolean(index + 1);
                                  checkNull(index + 1, rs);
                                  return (T) value;
                                } else if (Instant.class.equals(type)) {
                                  return (T) rs.getTimestamp(index + 1).toInstant();
                                } else {
                                  throw new IllegalArgumentException(
                                      "Unsupported data type " + type);
                                }
                              } catch (SQLException e) {
                                throw new RuntimeException(e);
                              }
                            }
                          }));
                }
                return results;
              }
            });
      }
    };
  }

  private void checkNull(int index, ResultSet rs) throws SQLException {
    if (rs.wasNull()) {
      throw new NullPointerException("Unexpected null value for index " + index + " in table");
    }
  }
}
