package com.gruelbox.transactionoutbox;

public class H2Dialect extends BaseDialect {
  H2Dialect() {
    super();
    changeMigration(5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)");
    changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked");
    disableMigration(8);
  }

  @Override
  public String getName() {
    return "H2";
  }
}
