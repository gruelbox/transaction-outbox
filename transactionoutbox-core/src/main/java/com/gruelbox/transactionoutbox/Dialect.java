package com.gruelbox.transactionoutbox;

import lombok.Getter;

/**
 * The SQL dialects supported by {@link DefaultPersistor}. Currently, this is only used to determine
 * whether {@code SKIP LOCKED} is available, so using the wrong dialect may work for unsupported
 * database platforms. However, in future this is likely to extend to other SQL features and
 * possibly be expanded to an interface to allow easier extension.
 */
@Getter
@Beta
public enum Dialect {
  MY_SQL_5(
      Constants.DEFAULT_SELECT_AND_LOCK_ENTRY_WITHOUT_SKIP_LOCKED_STMT,
      Constants.DEFAULT_SELECT_BATCH_ENTRIES_WITHOUT_SKIP_LOCKED_STMT), //
  MY_SQL_8(), //
  POSTGRESQL_9(
      Constants.DEFAULT_SELECT_AND_LOCK_ENTRY_STMT,
      Constants.DEFAULT_SELECT_BATCH_ENTRIES_STMT,
      Constants.DEFAULT_DELETE_ENTRY_STMT,
      "DELETE FROM {{table}} WHERE ctid IN (SELECT ctid FROM {{table}} WHERE nextAttemptTime < ? "
          + "AND processed = true AND blocked = false LIMIT {{batchSize}})"), //
  H2(
      Constants.DEFAULT_SELECT_AND_LOCK_ENTRY_WITHOUT_SKIP_LOCKED_STMT,
      Constants.DEFAULT_SELECT_BATCH_ENTRIES_WITHOUT_SKIP_LOCKED_STMT), //
  ORACLE(
      Constants.DEFAULT_SELECT_AND_LOCK_ENTRY_STMT,
      "SELECT {{fields}} FROM {{table}} WHERE nextAttemptTime < ? AND blocked = 0 AND processed =" +
          " 0 AND ROWNUM <= {{batchSize}} FOR UPDATE SKIP LOCKED",
      Constants.DEFAULT_DELETE_ENTRY_STMT,
      "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 AND ROWNUM <= {{batchSize}}"), //
  MSSQL(
      "SELECT id, invocation FROM {{table}} WITH (UPDLOCK, ROWLOCK, READPAST) WHERE id = ? AND version = ?",
      "SELECT TOP ({{batchSize}}) {{fields}}\n"
          + "FROM {{table}} WITH (UPDLOCK, ROWLOCK, READPAST)\n"
          + "WHERE nextAttemptTime < ? AND blocked = 0 AND processed = 0",
      "DELETE FROM {{table}} WITH (ROWLOCK, READPAST) WHERE id = ? and version = ?",
      "DELETE TOP ({{batchSize}}) FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND "
          + "blocked = 0",
      "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TXNO_VERSION')\n"
          + "BEGIN\n"
          + "    CREATE TABLE TXNO_VERSION (\n"
          + "        version INT\n"
          + "    );\n"
          + "END",
      "SELECT version FROM TXNO_VERSION WITH (UPDLOCK, HOLDLOCK, ROWLOCK)");

  /**
   * @return Format string for the SQL required to select and lock a record for processing. Should
   *     use ({@code SKIP LOCKED}) if supported by your dialect in order to increase performance
   *     when there are multiple instances of the application potentially competing to process the
   *     same task.
   */
  @SuppressWarnings("JavaDoc")
  private final String selectAndLockEntry;

  /**
   * @return Format string for the SQL required to select a batch of entries for processing. Should
   *     acquire locks and use ({@code SKIP LOCKED}) if supported by your dialect in order to
   *     increase performance when there are multiple instances of the application potentially
   *     competing to process the same task. Otherwise, no locking should be done.
   */
  @SuppressWarnings("JavaDoc")
  private final String selectBatchEntries;

  /**
   * @return Format string for the SQL required to delete an entry.
   */
  @SuppressWarnings("JavaDoc")
  private final String deleteEntry;

  /**
   * @return Format string for the SQL required to delete expired retained records.
   */
  @SuppressWarnings("JavaDoc")
  private final String deleteExpired;

  /**
   * @return SQL required to create version table if it doesn't already exist.
   */
  @SuppressWarnings("JavaDoc")
  private final String createVersionTableIfNotExists;

  /**
   * @return SQL required to select current version from version table and lock it.
   */
  @SuppressWarnings("JavaDoc")
  private final String selectCurrentVersionAndLockTable;

  Dialect() {
    this(Constants.DEFAULT_SELECT_AND_LOCK_ENTRY_STMT);
  }

  Dialect(String selectAndLockEntry) {
    this(selectAndLockEntry, Constants.DEFAULT_SELECT_BATCH_ENTRIES_STMT);
  }

  Dialect(String selectAndLockEntry, String selectBatchEntries) {
    this(selectAndLockEntry, selectBatchEntries, Constants.DEFAULT_DELETE_ENTRY_STMT);
  }

  Dialect(String selectAndLockEntry, String selectBatchEntries, String deleteEntry) {
    this(
        selectAndLockEntry, selectBatchEntries, deleteEntry, Constants.DEFAULT_DELETE_EXPIRED_STMT);
  }

  Dialect(
      String selectAndLockEntry,
      String selectBatchEntries,
      String deleteEntry,
      String deleteExpired) {
    this(
        selectAndLockEntry,
        selectBatchEntries,
        deleteEntry,
        deleteExpired,
        Constants.DEFAULT_CREATE_VERSION_TBL_STMT);
  }

  Dialect(
      String selectAndLockEntry,
      String selectBatchEntries,
      String deleteEntry,
      String deleteExpired,
      String createVersionTableIfNotExists) {
    this(
        selectAndLockEntry,
        selectBatchEntries,
        deleteEntry,
        deleteExpired,
        createVersionTableIfNotExists,
        Constants.DEFAULT_SELECT_CURRENT_VERSION_AND_LOCK_TBL_STMT);
  }

  Dialect(
      String selectAndLockEntry,
      String selectBatchEntries,
      String deleteEntry,
      String deleteExpired,
      String createVersionTableIfNotExists,
      String selectCurrentVersionAndLockTable) {
    this.selectAndLockEntry = selectAndLockEntry;
    this.selectBatchEntries = selectBatchEntries;
    this.deleteEntry = deleteEntry;
    this.deleteExpired = deleteExpired;
    this.createVersionTableIfNotExists = createVersionTableIfNotExists;
    this.selectCurrentVersionAndLockTable = selectCurrentVersionAndLockTable;
  }

  private static class Constants {

    static final String DEFAULT_SELECT_AND_LOCK_ENTRY_WITHOUT_SKIP_LOCKED_STMT =
        "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR UPDATE";

    static final String DEFAULT_SELECT_AND_LOCK_ENTRY_STMT =
        DEFAULT_SELECT_AND_LOCK_ENTRY_WITHOUT_SKIP_LOCKED_STMT + " SKIP LOCKED";

    static final String DEFAULT_SELECT_BATCH_ENTRIES_WITHOUT_SKIP_LOCKED_STMT =
        "SELECT {{fields}} FROM {{table}} WHERE nextAttemptTime < ? AND blocked = false AND "
            + "processed = false LIMIT {{batchSize}}";

    static final String DEFAULT_SELECT_BATCH_ENTRIES_STMT =
        DEFAULT_SELECT_BATCH_ENTRIES_WITHOUT_SKIP_LOCKED_STMT + " FOR UPDATE SKIP LOCKED";

    static final String DEFAULT_DELETE_ENTRY_STMT =
        "DELETE FROM {{table}} WHERE id = ? and version = ?";

    static final String DEFAULT_DELETE_EXPIRED_STMT =
        "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false"
            + " LIMIT {{batchSize}}";

    static final String DEFAULT_CREATE_VERSION_TBL_STMT =
        "CREATE TABLE IF NOT EXISTS TXNO_VERSION (version INT)";

    static final String DEFAULT_SELECT_CURRENT_VERSION_AND_LOCK_TBL_STMT =
        "SELECT version FROM TXNO_VERSION FOR UPDATE";
  }
}
