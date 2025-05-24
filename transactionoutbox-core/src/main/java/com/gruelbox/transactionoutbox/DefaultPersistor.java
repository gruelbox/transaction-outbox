package com.gruelbox.transactionoutbox;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
public class DefaultPersistor implements Persistor, Validatable {

  private static final String ALL_FIELDS =
      "id, uniqueRequestId, invocation, topic, seq, lastAttemptTime, nextAttemptTime, attempts, blocked, processed, version";

  /**
   * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a write
   *     lock. There's no point making this long; it's always better to just back off as quickly as
   *     possible and try another record. Generally these lock timeouts only kick in if the database
   *     does not support skip locking.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final int writeLockTimeoutSeconds = 2;

  /**
   * @param dialect The database dialect to use. Required.
   */
  @SuppressWarnings("JavaDoc")
  private final Dialect dialect;

  /**
   * @param tableName The database table name. The default is {@code TXNO_OUTBOX}.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final String tableName = "TXNO_OUTBOX";

  /**
   * @param migrate Set to false to disable automatic database migrations. This may be preferred if
   *     the default migration behaviour interferes with your existing toolset, and you prefer to
   *     manage the migrations explicitly (e.g. using FlyWay or Liquibase), or you do not give the
   *     application DDL permissions at runtime. You may use {@link #writeSchema(Writer)} to access
   *     the migrations.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final boolean migrate = true;

  /**
   * @param serializer The serializer to use for {@link Invocation}s. See {@link
   *     InvocationSerializer} for more information. Defaults to {@link
   *     InvocationSerializer#createDefaultJsonSerializer()} with no custom serializable classes.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  @Override
  public void validate(Validator validator) {
    validator.notNull("dialect", dialect);
    validator.notNull("tableName", tableName);
  }

  @Override
  public void migrate(TransactionManager transactionManager) {
    if (migrate) {
      DefaultMigrationManager.migrate(transactionManager, dialect);
    }
  }

  /**
   * Provides access to the database schema so that you may optionally use your existing toolset to
   * manage migrations.
   *
   * @param writer The writer to which the migrations are written.
   */
  public void writeSchema(Writer writer) {
    DefaultMigrationManager.writeSchema(writer, dialect);
  }

  @Override
  public void save(Transaction tx, TransactionOutboxEntry entry)
      throws SQLException, AlreadyScheduledException {
    var insertSql =
        "INSERT INTO "
            + tableName
            + " ("
            + ALL_FIELDS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    if (entry.getTopic() != null) {
      setNextSequence(tx, entry);
      log.info("Assigned sequence number {} to topic {}", entry.getSequence(), entry.getTopic());
    }
    PreparedStatement stmt = tx.prepareBatchStatement(insertSql);
    setupInsert(entry, writer, stmt);
    if (entry.getUniqueRequestId() == null) {
      stmt.addBatch();
      log.debug("Inserted {} in batch", entry.description());
    } else {
      try {
        stmt.executeUpdate();
        log.debug("Inserted {} immediately", entry.description());
      } catch (Exception e) {
        if (indexViolation(e)) {
          throw new AlreadyScheduledException(
              "Request " + entry.description() + " already exists", e);
        }
        throw e;
      }
    }
  }

  private void setNextSequence(Transaction tx, TransactionOutboxEntry entry) throws SQLException {
    //noinspection resource
    var seqSelect = tx.prepareBatchStatement(dialect.getFetchNextSequence());
    seqSelect.setString(1, entry.getTopic());
    try (ResultSet rs = seqSelect.executeQuery()) {
      if (rs.next()) {
        entry.setSequence(rs.getLong(1) + 1L);
        //noinspection resource
        var seqUpdate =
            tx.prepareBatchStatement("UPDATE TXNO_SEQUENCE SET seq = ? WHERE topic = ?");
        seqUpdate.setLong(1, entry.getSequence());
        seqUpdate.setString(2, entry.getTopic());
        seqUpdate.executeUpdate();
      } else {
        try {
          entry.setSequence(1L);
          //noinspection resource
          var seqInsert =
              tx.prepareBatchStatement("INSERT INTO TXNO_SEQUENCE (topic, seq) VALUES (?, ?)");
          seqInsert.setString(1, entry.getTopic());
          seqInsert.setLong(2, entry.getSequence());
          seqInsert.executeUpdate();
        } catch (Exception e) {
          if (indexViolation(e)) {
            setNextSequence(tx, entry);
          } else {
            throw e;
          }
        }
      }
    }
  }

  private boolean indexViolation(Exception e) {
    return (e instanceof SQLIntegrityConstraintViolationException)
        || (e.getClass().getName().equals("org.postgresql.util.PSQLException")
            && e.getMessage().contains("constraint"))
        || (e.getClass().getName().equals("com.microsoft.sqlserver.jdbc.SQLServerException")
            && e.getMessage().contains("duplicate key"));
  }

  private void setupInsert(
      TransactionOutboxEntry entry, StringWriter writer, PreparedStatement stmt)
      throws SQLException {
    stmt.setString(1, entry.getId());
    stmt.setString(2, entry.getUniqueRequestId());
    stmt.setString(3, writer.toString());
    stmt.setString(4, entry.getTopic() == null ? "*" : entry.getTopic());
    if (entry.getSequence() == null) {
      stmt.setObject(5, null);
    } else {
      stmt.setLong(5, entry.getSequence());
    }
    stmt.setTimestamp(
        6, entry.getLastAttemptTime() == null ? null : Timestamp.from(entry.getLastAttemptTime()));
    stmt.setTimestamp(7, Timestamp.from(entry.getNextAttemptTime()));
    stmt.setInt(8, entry.getAttempts());
    stmt.setBoolean(9, entry.isBlocked());
    stmt.setBoolean(10, entry.isProcessed());
    stmt.setInt(11, entry.getVersion());
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection().prepareStatement(dialect.getDelete().replace("{{table}}", tableName))) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      if (stmt.executeUpdate() != 1) {
        throw new OptimisticLockException();
      }
      log.debug("Deleted {}", entry.description());
    }
  }

  @Override
  public void deleteBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception {
    if (entries == null || entries.isEmpty()) {
      return;
    }

    try (PreparedStatement stmt =
        tx.connection().prepareStatement(dialect.getDelete().replace("{{table}}", tableName))) {

      for (TransactionOutboxEntry entry : entries) {
        stmt.setString(1, entry.getId());
        stmt.setInt(2, entry.getVersion());
        stmt.addBatch();
      }

      int[] results = stmt.executeBatch();

      for (int i = 0; i < results.length; i++) {
        if (results[i] != 1) {
          throw new OptimisticLockException();
        }
        log.debug("Batch deleted {}", entries.get(i).description());
      }
      log.debug("Batch deleted {} entries", results.length);
    }
  }

  @Override
  public void update(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
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
  public void updateBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception {
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                "UPDATE "
                    + tableName
                    + " SET "
                    + "lastAttemptTime = ?, nextAttemptTime = ?, attempts = ?, "
                    + "blocked = ?, processed = ?, version = ? "
                    + "WHERE id = ? AND version = ?")) {

      for (TransactionOutboxEntry entry : entries) {
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
        stmt.addBatch();
      }

      int[] results = stmt.executeBatch();

      for (int i = 0; i < results.length; i++) {
        if (results[i] != 1) {
          throw new OptimisticLockException();
        }
        entries.get(i).setVersion(entries.get(i).getVersion() + 1);
        log.debug("Batch updated {}", entries.get(i).description());
      }
      log.debug("Batch updated {} entries", results.length);
    }
  }

  @Override
  public boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect
                    .getLock()
                    .replace("{{table}}", tableName)
                    .replace("{{allFields}}", ALL_FIELDS))) {
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
  public boolean lockBatch(Transaction tx, List<TransactionOutboxEntry> entries) throws Exception {
    if (entries == null || entries.isEmpty()) {
      return true; // Nothing to lock is considered success
    }

    // Create placeholders for each entry
    String placeholders = entries.stream().map(e -> "(?, ?)").collect(Collectors.joining(", "));

    // Get the SQL from the dialect, replacing the placeholders
    String sql =
        dialect
            .getLockBatch()
            .replace("{{table}}", tableName)
            .replace("{{placeholders}}", placeholders);

    try (PreparedStatement stmt = tx.connection().prepareStatement(sql)) {
      // Set parameters for each entry
      int paramIndex = 1;
      for (TransactionOutboxEntry entry : entries) {
        stmt.setString(paramIndex++, entry.getId());
        stmt.setInt(paramIndex++, entry.getVersion());
      }

      stmt.setQueryTimeout(writeLockTimeoutSeconds);

      try {
        try (ResultSet rs = stmt.executeQuery()) {
          // Build a map of id -> deserialized invocation
          Map<String, Invocation> invocationsById = new HashMap<>();

          while (rs.next()) {
            String id = rs.getString("id");
            try (Reader invocationStream = rs.getCharacterStream("invocation")) {
              Invocation invocation = serializer.deserializeInvocation(invocationStream);
              invocationsById.put(id, invocation);
            }
          }

          // If we didn't get all entries, return false
          if (invocationsById.size() != entries.size()) {
            log.debug(
                "Could only lock {} out of {} entries", invocationsById.size(), entries.size());
            return false;
          }

          // Update each entry with its deserialized invocation
          for (TransactionOutboxEntry entry : entries) {
            Invocation invocation = invocationsById.get(entry.getId());
            if (invocation == null) {
              log.error("Could not find result for entry {}", entry.getId());
              return false;
            }
            entry.setInvocation(invocation);
          }

          return true;
        }
      } catch (SQLTimeoutException e) {
        log.debug("Lock attempt timed out on batch of {} entries", entries.size());
        return false;
      }
    }
  }

  @Override
  public boolean unblock(Transaction tx, String entryId) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                "UPDATE "
                    + tableName
                    + " SET attempts = 0, blocked = "
                    + dialect.booleanValue(false)
                    + " "
                    + "WHERE blocked = "
                    + dialect.booleanValue(true)
                    + " AND processed = "
                    + dialect.booleanValue(false)
                    + " AND id = ?")) {
      stmt.setString(1, entryId);
      stmt.setQueryTimeout(writeLockTimeoutSeconds);
      return stmt.executeUpdate() != 0;
    }
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now)
      throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect
                    .getSelectBatch()
                    .replace("{{table}}", tableName)
                    .replace("{{batchSize}}", Integer.toString(batchSize))
                    .replace("{{allFields}}", ALL_FIELDS))) {
      stmt.setTimestamp(1, Timestamp.from(now));
      var result = new ArrayList<TransactionOutboxEntry>(batchSize);
      gatherResults(stmt, result);
      return result;
    }
  }

  @Override
  public Collection<TransactionOutboxEntry> selectNextInTopics(
      Transaction tx, int batchSize, Instant now) throws Exception {
    var sql =
        dialect
            .getFetchNextInAllTopics()
            .replace("{{table}}", tableName)
            .replace("{{batchSize}}", Integer.toString(batchSize))
            .replace("{{allFields}}", ALL_FIELDS);
    //noinspection resource
    try (PreparedStatement stmt = tx.connection().prepareStatement(sql)) {
      stmt.setTimestamp(1, Timestamp.from(now));
      var results = new ArrayList<TransactionOutboxEntry>();
      gatherResults(stmt, results);
      return results;
    }
  }

  @Override
  public Collection<TransactionOutboxEntry> selectNextInSelectedTopics(
      Transaction tx, List<String> topicNames, int batchSize, Instant now) throws Exception {

    var topicsInParameterList = topicNames.stream().map(it -> "?").collect(Collectors.joining(","));
    var sql =
        dialect
            .getFetchNextInSelectedTopics()
            .replace("{{table}}", tableName)
            .replace("{{topicNames}}", topicsInParameterList)
            .replace("{{batchSize}}", Integer.toString(batchSize))
            .replace("{{allFields}}", ALL_FIELDS);
    //noinspection resource
    try (PreparedStatement stmt = tx.connection().prepareStatement(sql)) {
      var counter = 1;
      for (var topicName : topicNames) {
        stmt.setString(counter, topicName);
        counter++;
      }
      stmt.setTimestamp(counter, Timestamp.from(now));
      var results = new ArrayList<TransactionOutboxEntry>();
      gatherResults(stmt, results);
      return results;
    }
  }

  /**
   * Selects the next batch of entries in topics, maintaining order within each topic. This method
   * is used for ordered batch processing and returns multiple entries per topic up to the batch
   * size limit.
   *
   * @param tx The current transaction
   * @param batchSize The maximum number of entries to return per topic
   * @param now The current time
   * @return A collection of entries ordered by topic and sequence
   * @throws Exception If an error occurs during selection
   */
  public Collection<TransactionOutboxEntry> selectNextBatchInTopics(
      Transaction tx, int batchSize, Instant now) throws Exception {
    var sql =
        dialect
            .getFetchNextBatchInTopics()
            .replace("{{table}}", tableName)
            .replace("{{batchSize}}", Integer.toString(batchSize))
            .replace("{{allFields}}", ALL_FIELDS);
    log.debug("SQL: {}", sql);
    //noinspection resource
    try (PreparedStatement stmt = tx.connection().prepareStatement(sql)) {
      stmt.setTimestamp(1, Timestamp.from(now));
      var results = new ArrayList<TransactionOutboxEntry>();
      gatherResults(stmt, results);
      return results;
    }
  }

  @Override
  public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now)
      throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect
                    .getDeleteExpired()
                    .replace("{{table}}", tableName)
                    .replace("{{batchSize}}", Integer.toString(batchSize)))) {
      stmt.setTimestamp(1, Timestamp.from(now));
      return stmt.executeUpdate();
    }
  }

  private void gatherResults(PreparedStatement stmt, Collection<TransactionOutboxEntry> output)
      throws SQLException, IOException {
    try (ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        output.add(map(rs));
      }
      log.debug("Found {} results", output.size());
    }
  }

  private TransactionOutboxEntry map(ResultSet rs) throws SQLException, IOException {
    String topic = rs.getString("topic");
    Long sequence = rs.getLong("seq");
    if (rs.wasNull()) {
      sequence = null;
    }
    // Reading invocationStream *must* occur first because some drivers (ex. SQL Server)
    // implement true streams that are not buffered in memory. Calling any other getter
    // on ResultSet before invocationStream is read will cause Reader to be closed
    // prematurely.
    try (Reader invocationStream = rs.getCharacterStream("invocation")) {
      Invocation invocation;
      try {
        invocation = serializer.deserializeInvocation(invocationStream);
      } catch (IOException e) {
        invocation = new FailedDeserializingInvocation(e);
      }
      TransactionOutboxEntry entry =
          TransactionOutboxEntry.builder()
              .invocation(invocation)
              .id(rs.getString("id"))
              .uniqueRequestId(rs.getString("uniqueRequestId"))
              .topic("*".equals(topic) ? null : topic)
              .sequence(sequence)
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
  @Override
  public void clear(Transaction tx) throws SQLException {
    //noinspection resource
    try (Statement stmt = tx.connection().createStatement()) {
      stmt.execute("DELETE FROM " + tableName);
    }
  }

  @Override
  public boolean checkConnection(Transaction tx) throws SQLException {
    //noinspection resource
    try (Statement stmt = tx.connection().createStatement();
        ResultSet rs = stmt.executeQuery(dialect.getCheckSql())) {
      return rs.next() && (rs.getInt(1) == 1);
    }
  }
}
