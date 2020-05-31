package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.SqlPersistor;
import com.gruelbox.transactionoutbox.SqlPersistor.Binder;
import com.gruelbox.transactionoutbox.SqlPersistor.ResultRow;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JdbcSqlHandler implements SqlPersistor.Handler<Connection, JdbcTransaction<?>> {

  @Override
  public <T> T statement(
      JdbcTransaction<?> tx,
      Dialect dialect,
      String sql,
      boolean batchable,
      Function<Binder, T> binding) {
    log.debug("Executing: {}", sql);
    if (batchable) {
      return executeWithSharedStatement(tx, sql, binding);
    } else {
      return executeWithStandaloneStatement(tx, sql, binding);
    }
  }

  @Override
  public void handleSaveException(TransactionOutboxEntry entry, Throwable t) {
    try {
      if (t instanceof CompletionException) {
        throw t.getCause();
      } else {
        throw t;
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      throw new AlreadyScheduledException("Request " + entry.description() + " already exists", e);
    } catch (Throwable e) {
      if (e.getClass().getName().equals("org.postgresql.util.PSQLException")
          && e.getMessage().contains("constraint")) {
        throw new AlreadyScheduledException(
            "Request " + entry.description() + " already exists", e.getCause());
      }
      throw (RuntimeException) Utils.uncheckAndThrow(e);
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
    return new SqlPersistor.Binder() {

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
      public CompletableFuture<Integer> execute() {
        return toBlockingFuture((Callable<Integer>) statement::executeUpdate);
      }

      @Override
      public <T> CompletableFuture<List<T>> executeQuery(
          int expectedRowCount, Function<ResultRow, T> rowMapper) {
        return toBlockingFuture(
            () -> {
              List<T> results = new ArrayList<>(expectedRowCount);
              try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                  log.debug("Fetched row");
                  results.add(
                      rowMapper.apply(
                          new ResultRow() {
                            @SuppressWarnings({"unchecked", "WrapperTypeMayBePrimitive"})
                            @Override
                            public <V> V get(int index, Class<V> type) {
                              try {
                                if (String.class.equals(type)) {
                                  return (V) rs.getString(index + 1);
                                } else if (Integer.class.equals(type)) {
                                  Integer value = rs.getInt(index + 1);
                                  checkNull(index + 1, rs);
                                  return (V) value;
                                } else if (Boolean.class.equals(type)) {
                                  Boolean value = rs.getBoolean(index + 1);
                                  checkNull(index + 1, rs);
                                  return (V) value;
                                } else if (Instant.class.equals(type)) {
                                  return (V) rs.getTimestamp(index + 1).toInstant();
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
