package com.gruelbox.transactionoutbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Dialect {
  MY_SQL_5(false),
  MY_SQL_8(true),
  POSTGRESQL_9(true),
  H2(false);

  /**
   * Enables use of hot row support ({@code SKIP LOCKED}) on Postgres or MySQL 8+, greatly improving
   * performance.
   */
  private final boolean supportsSkipLock;
}
