package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.toBlockingFuture;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.PessimisticLockException;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.Utils;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlApi;
import com.gruelbox.transactionoutbox.sql.SqlResultRow;
import com.gruelbox.transactionoutbox.sql.SqlStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTimeoutException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JdbcSqlApi implements SqlApi<Connection, JdbcTransaction> {

  @Override
  public boolean requiresNativeStatementMapping() {
    return false;
  }

  @Override
  public <T> T statement(
      JdbcTransaction tx,
      Dialect dialect,
      String sql,
      int timeoutSeconds,
      boolean batchable,
      Function<SqlStatement, T> binding) {
    log.debug("Executing: {}", sql);
    if (batchable) {
      return executeWithSharedStatement(tx, sql, timeoutSeconds, binding);
    } else {
      return executeWithStandaloneStatement(tx, sql, timeoutSeconds, binding);
    }
  }

  @Override
  public Throwable mapSaveException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (SQLIntegrityConstraintViolationException e) {
      return new AlreadyScheduledException("Request " + entry.description() + " already exists", e);
    } catch (Throwable e) {
      if (e.getClass().getName().equals("org.postgresql.util.PSQLException")
          && e.getMessage().contains("constraint")) {
        return new AlreadyScheduledException(
            "Request " + entry.description() + " already exists", e.getCause());
      }
      return e;
    }
  }

  @Override
  public Throwable mapLockException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (TimeoutException | SQLTimeoutException e) {
      return new PessimisticLockException(t);
    } catch (Throwable e) {
      if (e.getMessage().contains("timeout")) {
        return new PessimisticLockException(t);
      }
      return e;
    }
  }

  @Override
  public Throwable mapUpdateException(TransactionOutboxEntry entry, Throwable t) {
    try {
      throw underlying(t);
    } catch (TimeoutException | SQLTimeoutException e) {
      return new PessimisticLockException(t);
    } catch (Throwable e) {
      if (e.getMessage().contains("timeout")) {
        return new PessimisticLockException(t);
      }
      return e;
    }
  }

  private <T> T executeWithStandaloneStatement(
      JdbcTransaction tx, String sql, int timeoutSeconds, Function<SqlStatement, T> binding) {
    try (PreparedStatement statement = tx.connection().prepareStatement(sql)) {
      statement.setQueryTimeout(timeoutSeconds);
      return binding.apply(executeAndMap(statement));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T executeWithSharedStatement(
      JdbcTransaction tx, String sql, int timeoutSeconds, Function<SqlStatement, T> binding) {
    PreparedStatement statement = tx.prepareBatchStatement(sql);
    try {
      statement.setQueryTimeout(timeoutSeconds);
    } catch (SQLException e) {
      Utils.uncheckAndThrow(e);
    }
    return binding.apply(executeAndMap(statement));
  }

  private SqlStatement executeAndMap(PreparedStatement statement) {
    return new SqlStatement() {

      @Override
      public SqlStatement bind(int index, Object value) {
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
          int expectedRowCount, Function<SqlResultRow, T> rowMapper) {
        return toBlockingFuture(
            () -> {
              List<T> results = new ArrayList<>(expectedRowCount);
              try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                  log.debug("Fetched row");
                  results.add(
                      rowMapper.apply(
                          new SqlResultRow() {
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

  Throwable underlying(Throwable t) {
    if (t instanceof CompletionException) {
      return t.getCause();
    } else {
      return t;
    }
  }
}
