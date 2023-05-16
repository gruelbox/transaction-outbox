package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.ThreadLocalContextTransactionManager;
import com.gruelbox.transactionoutbox.ThrowingRunnable;
import com.gruelbox.transactionoutbox.Transaction;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.UncheckedException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

@Slf4j
class TestUtils {

  private static final Table<Record> TEST_TABLE = DSL.table("TEST_TABLE");

  @SuppressWarnings("SameParameterValue")
  static void runSql(TransactionManager transactionManager, String sql) {
    transactionManager.inTransaction(
        tx -> {
          try {
            try (Statement statement = tx.connection().createStatement()) {
              statement.execute(sql);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  static void uncheck(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      uncheckAndThrow(e);
    }
  }

  static <T> T uncheckAndThrow(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    throw new UncheckedException(e);
  }

  static void createTestTable(DSLContext dsl) {
    log.info("Creating table");
    dsl.dropTableIfExists(TEST_TABLE).execute();
    dsl.createTable(TEST_TABLE).column("VAL", SQLDataType.INTEGER).execute();
  }

  static void writeRecord(Configuration configuration, int value) {
    log.info("Inserting record {}", value);
    configuration.dsl().insertInto(TEST_TABLE).values(value).execute();
  }

  static void writeRecord(Transaction transaction, int value) {
    Configuration configuration = transaction.context();
    writeRecord(configuration, value);
  }

  static void writeRecord(ThreadLocalContextTransactionManager transactionManager, int value) {
    transactionManager.requireTransaction(tx -> writeRecord(tx, value));
  }

  static void assertRecordExists(DSLContext dsl, int value) {
    assertTrue(
        dsl.select()
            .from(TEST_TABLE)
            .where(DSL.field("VAL").eq(value))
            .fetchOptional()
            .isPresent());
  }

  static void assertRecordNotExists(DSLContext dsl, int value) {
    assertFalse(
        dsl.select()
            .from(TEST_TABLE)
            .where(DSL.field("VAL").eq(value))
            .fetchOptional()
            .isPresent());
  }
}
