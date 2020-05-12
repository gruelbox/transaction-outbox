package com.gruelbox.transactionoutbox;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
 * The default {@link Persistor} for {@link TransactionOutbox}. Most methods are open for extension,
 * or you can modify various options through the builder.
 */
@Slf4j
@SuperBuilder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class DefaultPersistor implements Persistor {

  private static final String SELECT_ALL =
      // language=MySQL
      "SELECT id, invocation, nextAttemptTime, attempts, blacklisted, version FROM TXNO_OUTBOX";

  /** Only wait for 2 second before giving up on obtaining an entry lock. There's no point hanging around. */
  private static final int LOCK_TIMEOUT_SECONDS = 2;

  /** The database dialect to use. Required. */
  @NotNull private final Dialect dialect;

  /**
   * The serializer to use for {@link Invocation}s. See {@link InvocationSerializer} for more
   * information. Defaults to {@link InvocationSerializer#createDefaultJsonSerializer()}.
   */
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  @Override
  public void migrate(TransactionManager transactionManager) {
    DefaultMigrationManager.migrate(transactionManager);
  }

  @Override
  public void save(Transaction tx, TransactionOutboxEntry entry) throws SQLException {
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    PreparedStatement stmt =
        tx.prepareBatchStatement("INSERT INTO TXNO_OUTBOX VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setString(1, entry.getId());
    stmt.setString(2, writer.toString());
    stmt.setTimestamp(3, Timestamp.from(entry.getNextAttemptTime()));
    stmt.setInt(4, entry.getAttempts());
    stmt.setBoolean(5, entry.isBlacklisted());
    stmt.setInt(6, entry.getVersion());
    stmt.addBatch();
    log.debug("Inserted {} in batch", entry.description());
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    try (PreparedStatement stmt =
        // language=MySQL
        tx.connection().prepareStatement("DELETE FROM TXNO_OUTBOX WHERE id = ? and version = ?")) {
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
                "UPDATE TXNO_OUTBOX "
                    + "SET nextAttemptTime = ?, attempts = ?, blacklisted = ?, version = ? "
                    + "WHERE id = ? and version = ?")) {
      stmt.setTimestamp(1, Timestamp.from(entry.getNextAttemptTime()));
      stmt.setInt(2, entry.getAttempts());
      stmt.setBoolean(3, entry.isBlacklisted());
      stmt.setInt(4, entry.getVersion() + 1);
      stmt.setString(5, entry.getId());
      stmt.setInt(6, entry.getVersion());
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
                    ? "SELECT id FROM TXNO_OUTBOX WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED"
                    // language=MySQL
                    : "SELECT id FROM TXNO_OUTBOX WHERE id = ? AND version = ? FOR UPDATE")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      stmt.setQueryTimeout(LOCK_TIMEOUT_SECONDS);
      return gotRecord(entry, stmt);
    }
  }

  @Override
  public boolean whitelist(Transaction tx, String entryId) throws Exception {
    PreparedStatement stmt =
        tx.prepareBatchStatement(
            "UPDATE TXNO_OUTBOX SET attempts = 0, blacklisted = false "
                + "WHERE blacklisted = true AND id = ?");
    stmt.setString(1, entryId);
    stmt.setQueryTimeout(LOCK_TIMEOUT_SECONDS);
    return stmt.executeUpdate() != 0;
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                SELECT_ALL + " WHERE nextAttemptTime < ? AND blacklisted = false LIMIT ?")) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
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

  private boolean gotRecord(TransactionOutboxEntry entry, PreparedStatement stmt)
      throws SQLException {
    try {
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next();
      }
    } catch (SQLTimeoutException e) {
      log.debug("Lock attempt timed out on {}", entry.description());
      return false;
    }
  }

  private TransactionOutboxEntry map(ResultSet rs) throws SQLException, IOException {
    try (Reader invocationStream = rs.getCharacterStream("invocation")) {
      TransactionOutboxEntry entry =
          TransactionOutboxEntry.builder()
              .id(rs.getString("id"))
              .invocation(serializer.deserializeInvocation(invocationStream))
              .nextAttemptTime(rs.getTimestamp("nextAttemptTime").toInstant())
              .attempts(rs.getInt("attempts"))
              .blacklisted(rs.getBoolean("blacklisted"))
              .version(rs.getInt("version"))
              .build();
      log.debug("Found {}", entry);
      return entry;
    }
  }

  // For testing. Assumed low volume.
  void clear(Transaction tx) throws SQLException {
    try (Statement stmt = tx.connection().createStatement()) {
      stmt.execute("DELETE FROM TXNO_OUTBOX");
    }
  }
}
