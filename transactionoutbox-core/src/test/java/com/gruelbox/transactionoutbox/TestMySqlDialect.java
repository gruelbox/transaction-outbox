package com.gruelbox.transactionoutbox;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TestMySqlDialect {

  private static final String MIGRATION_14_SQL =
      "ALTER TABLE TXNO_OUTBOX MODIFY COLUMN blocked BOOLEAN";

  @Test
  void mySqlDialectsFixBlockedColumnTypeInMigration14() {
    assertEquals(MIGRATION_14_SQL, migration14(Dialect.MY_SQL_5).getSql());
    assertEquals(MIGRATION_14_SQL, migration14(Dialect.MY_SQL_8).getSql());
  }

  @Test
  void nonMySqlDialectsHaveNoMigration14Sql() {
    assertThat(migration14(Dialect.POSTGRESQL_9).getSql(), nullValue());
    assertThat(migration14(Dialect.H2).getSql(), nullValue());
    assertThat(migration14(Dialect.ORACLE).getSql(), nullValue());
    assertThat(migration14(Dialect.MS_SQL_SERVER).getSql(), nullValue());
  }

  @Test
  void mySqlQueriesCompareBlockedAsBoolean() {
    for (Dialect dialect : new Dialect[] {Dialect.MY_SQL_5, Dialect.MY_SQL_8}) {
      assertThat(dialect.getDeleteExpired(), containsString("blocked = false"));
      assertThat(dialect.getDeleteExpired(), not(containsString("blocked = '0'")));
      assertThat(dialect.getSelectBatch(), containsString("blocked = false"));
      assertThat(dialect.getSelectBatch(), not(containsString("blocked = '0'")));
    }
  }

  private static Migration migration14(Dialect dialect) {
    return dialect
        .getMigrations()
        .filter(migration -> migration.getVersion() == 14)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Migration 14 not found for " + dialect));
  }
}
