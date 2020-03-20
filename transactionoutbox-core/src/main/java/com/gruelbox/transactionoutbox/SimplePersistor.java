package com.gruelbox.transactionoutbox;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Xpp3Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SimplePersistor implements Persistor {

  /** TODO security */
  private static final XStream X_STREAM = new XStream(new Xpp3Driver());

  private static final String SELECT_ALL =
      "SELECT id, invocation, nextAttemptTime, attempts, blacklisted, version FROM TXNO_OUTBOX";

  @Override
  public void save(Transaction tx, TransactionOutboxEntry entry) throws SQLException {
    Utils.validate(entry);
    String serializedForm = X_STREAM.toXML(entry.getInvocation());
    PreparedStatement stmt =
        tx.prepareBatchStatement("INSERT INTO TXNO_OUTBOX VALUES (?, ?, ?, ?, ?, ?)");
    stmt.setString(1, entry.getId());
    stmt.setString(2, serializedForm);
    stmt.setTimestamp(3, Timestamp.valueOf(entry.getNextAttemptTime()));
    stmt.setInt(4, entry.getAttempts());
    stmt.setBoolean(5, entry.isBlacklisted());
    stmt.setInt(6, entry.getVersion());
    stmt.addBatch();
    log.debug("Inserted {} in batch", entry.description());
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    Utils.validate(entry);
    try (PreparedStatement stmt =
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
    Utils.validate(entry);
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                "UPDATE TXNO_OUTBOX "
                    + "SET nextAttemptTime = ?, attempts = ?, blacklisted = ?, version = ? "
                    + "WHERE id = ? and version = ?")) {
      stmt.setTimestamp(1, Timestamp.valueOf(entry.getNextAttemptTime()));
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
                "SELECT id FROM TXNO_OUTBOX WHERE id = ? AND version = ? FOR UPDATE")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      stmt.setQueryTimeout(5);
      return gotRecord(entry, stmt);
    }
  }

  @Override
  public boolean lockSkippingLocks(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                "SELECT id FROM TXNO_OUTBOX WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      stmt.setQueryTimeout(5);
      return gotRecord(entry, stmt);
    }
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                SELECT_ALL + " WHERE nextAttemptTime <= ? AND blacklisted = false LIMIT ?")) {
      stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
    }
  }

  @Override
  public List<TransactionOutboxEntry> selectBatchSkippingLocksForUpdate(
      Transaction tx, int batchSize) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                SELECT_ALL + " WHERE nextAttemptTime < ? AND blacklisted = false LIMIT ?")) {
      stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
    }
  }

  private List<TransactionOutboxEntry> gatherResults(int batchSize, PreparedStatement stmt)
      throws SQLException {
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

  private TransactionOutboxEntry map(ResultSet rs) throws SQLException {
    TransactionOutboxEntry entry =
        TransactionOutboxEntry.builder()
            .id(rs.getString("id"))
            .invocation((Invocation) X_STREAM.fromXML(rs.getCharacterStream("invocation")))
            .nextAttemptTime(rs.getTimestamp("nextAttemptTime").toLocalDateTime())
            .attempts(rs.getInt("attempts"))
            .blacklisted(rs.getBoolean("blacklisted"))
            .version(rs.getInt("version"))
            .build();
    log.debug("Found {}", entry);
    return entry;
  }
}
