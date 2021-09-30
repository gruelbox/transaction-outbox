package com.gruelbox.transactionoutbox;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * The default {@link Persistor} for {@link TransactionOutbox}.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link DefaultPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>More significant changes can be achieved by subclassing, which is explicitly supported. If, on
 * the other hand, you want to use a completely non-relational underlying data store or do something
 * equally esoteric, you may prefer to implement {@link Persistor} from the ground up.
 */
@Slf4j
@SuperBuilder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class DefaultPersistor implements Persistor {

  private static final String ALL_FIELDS =
      "id, uniqueRequestId, invocation, lastAttemptTime, nextAttemptTime, attempts, blocked, processed, version";

  /**
   * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a write
   *     lock. There's no point making this long; it's always better to just back off as quickly as
   *     possible and try another record. Generally these lock timeouts only kick in if {@link
   *     Dialect#isSupportsSkipLock()} is false.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final int writeLockTimeoutSeconds = 2;

  /** @param dialect The database dialect to use. Required. */
  @SuppressWarnings("JavaDoc")
  @NotNull
  private final Dialect dialect;

  /** @param tableName The database table name. The default is {@code TXNO_OUTBOX}. */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final String tableName = "TXNO_OUTBOX";

  /**
   * @param migrate Set to false to disable automatic database migrations. This may be preferred if
   *     the default migration behaviour interferes with your existing toolset, and you prefer to
   *     manage the migrations explicitly (e.g. using FlyWay or Liquibase), or your do not give the
   *     application DDL permissions at runtime.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final boolean migrate = true;

  /**
   * @param serializer The serializer to use for {@link Invocation}s. See {@link
   *     InvocationSerializer} for more information. Defaults to {@link
   *     InvocationSerializer#createDefaultJsonSerializer()} with no custom serializable classes..
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  @Override
  public void migrate(TransactionManager transactionManager) {
    if (migrate) {
      DefaultMigrationManager.migrate(transactionManager, dialect);
    }
  }

  @Override
  public void save(Transaction tx, TransactionOutboxEntry entry)
      throws SQLException, AlreadyScheduledException {
    var insertSql =
        "INSERT INTO " + tableName + " (" + ALL_FIELDS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    if (entry.getUniqueRequestId() == null) {
      PreparedStatement stmt = tx.prepareBatchStatement(insertSql);
      setupInsert(entry, writer, stmt);
      stmt.addBatch();
      log.debug("Inserted {} in batch", entry.description());
    } else {
      try (PreparedStatement stmt = tx.connection().prepareStatement(insertSql)) {
        setupInsert(entry, writer, stmt);
        stmt.executeUpdate();
        log.debug("Inserted {} immediately", entry.description());
      } catch (SQLIntegrityConstraintViolationException e) {
        throw new AlreadyScheduledException(
            "Request " + entry.description() + " already exists", e);
      } catch (Exception e) {
        if (e.getClass().getName().equals("org.postgresql.util.PSQLException")
            && e.getMessage().contains("constraint")) {
          throw new AlreadyScheduledException(
              "Request " + entry.description() + " already exists", e);
        }
        throw e;
      }
    }
  }

  private void setupInsert(
      TransactionOutboxEntry entry, StringWriter writer, PreparedStatement stmt)
      throws SQLException {
    stmt.setString(1, entry.getId());
    stmt.setString(2, entry.getUniqueRequestId());
    stmt.setString(3, writer.toString());
    stmt.setTimestamp(
        4, entry.getLastAttemptTime() == null ? null : Timestamp.from(entry.getLastAttemptTime()));
    stmt.setTimestamp(5, Timestamp.from(entry.getNextAttemptTime()));
    stmt.setInt(6, entry.getAttempts());
    stmt.setBoolean(7, entry.isBlocked());
    stmt.setBoolean(8, entry.isProcessed());
    stmt.setInt(9, entry.getVersion());
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    try (PreparedStatement stmt =
        // language=MySQL
        tx.connection()
            .prepareStatement("DELETE FROM " + tableName + " WHERE id = ? and version = ?")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      if (stmt.executeUpdate() != 1) {
        throw new OptimisticLockException();
      }
      log.debug("Deleted {}", entry.description());
    }
  }

  @Override
  public void update(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                "UPDATE "
                    + tableName
                    + " "
                    + "SET lastAttemptTime = ?, nextAttemptTime = ?, attempts = ?, blocked = ?, processed = ?, version = ? "
                    + "WHERE id = ? and version = ?")) {
      stmt.setTimestamp(
          1,
          entry.getLastAttemptTime() == null ? null : Timestamp.from(entry.getLastAttemptTime()));
      stmt.setTimestamp(2, Timestamp.from(entry.getNextAttemptTime()));
      stmt.setInt(3, entry.getAttempts());
      stmt.setBoolean(4, entry.isBlocked());
      stmt.setBoolean(5, entry.isProcessed());
      stmt.setInt(6, entry.getVersion() + 1);
      stmt.setString(7, entry.getId());
      stmt.setInt(8, entry.getVersion());
      if (stmt.executeUpdate() != 1) {
        throw new OptimisticLockException();
      }
      entry.setVersion(entry.getVersion() + 1);
      log.debug("Updated {}", entry.description());
    }
  }

  @Override
  public boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect.isSupportsSkipLock()
                    // language=MySQL
                    ? "SELECT id, invocation FROM "
                        + tableName
                        + " WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED"
                    // language=MySQL
                    : "SELECT id, invocation FROM "
                        + tableName
                        + " WHERE id = ? AND version = ? FOR UPDATE")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      stmt.setQueryTimeout(writeLockTimeoutSeconds);
      try {
        try (ResultSet rs = stmt.executeQuery()) {
          if (!rs.next()) {
            return false;
          }
          // Ensure that subsequent processing uses a deserialized invocation rather than
          // the object from the caller, which might not serialize well and thus cause a
          // difference between immediate and retry processing
          try (Reader invocationStream = rs.getCharacterStream("invocation")) {
            entry.setInvocation(serializer.deserializeInvocation(invocationStream));
          }
          return true;
        }
      } catch (SQLTimeoutException e) {
        log.debug("Lock attempt timed out on {}", entry.description());
        return false;
      }
    }
  }

  @Override
  public boolean unblock(Transaction tx, String entryId) throws Exception {
    PreparedStatement stmt =
        tx.prepareBatchStatement(
            "UPDATE "
                + tableName
                + " SET attempts = 0, blocked = false "
                + "WHERE blocked = true AND processed = false AND id = ?");
    stmt.setString(1, entryId);
    stmt.setQueryTimeout(writeLockTimeoutSeconds);
    return stmt.executeUpdate() != 0;
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now)
      throws Exception {
    String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                "SELECT "
                    + ALL_FIELDS
                    + " FROM "
                    + tableName
                    + " WHERE nextAttemptTime < ? AND blocked = false AND processed = false LIMIT ?"
                    + forUpdate)) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
    }
  }

  @Override
  public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now)
      throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(dialect.getDeleteExpired().replace("{{table}}", tableName))) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return stmt.executeUpdate();
    }
  }

  private List<TransactionOutboxEntry> gatherResults(int batchSize, PreparedStatement stmt)
      throws SQLException, IOException {
    try (ResultSet rs = stmt.executeQuery()) {
      ArrayList<TransactionOutboxEntry> result = new ArrayList<>(batchSize);
      while (rs.next()) {
        result.add(map(rs));
      }
      log.debug("Found {} results", result.size());
      return result;
    }
  }

  private TransactionOutboxEntry map(ResultSet rs) throws SQLException, IOException {
    try (Reader invocationStream = rs.getCharacterStream("invocation")) {
      TransactionOutboxEntry entry =
          TransactionOutboxEntry.builder()
              .id(rs.getString("id"))
              .uniqueRequestId(rs.getString("uniqueRequestId"))
              .invocation(serializer.deserializeInvocation(invocationStream))
              .lastAttemptTime(
                  rs.getTimestamp("lastAttemptTime") == null
                      ? null
                      : rs.getTimestamp("lastAttemptTime").toInstant())
              .nextAttemptTime(rs.getTimestamp("nextAttemptTime").toInstant())
              .attempts(rs.getInt("attempts"))
              .blocked(rs.getBoolean("blocked"))
              .processed(rs.getBoolean("processed"))
              .version(rs.getInt("version"))
              .build();
      log.debug("Found {}", entry);
      return entry;
    }
  }

  // For testing. Assumed low volume.
  public void clear(Transaction tx) throws SQLException {
    try (Statement stmt = tx.connection().createStatement()) {
      stmt.execute("DELETE FROM " + tableName);
    }
  }
}
