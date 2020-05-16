package com.gruelbox.transactionoutbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The SQL dialects supported by {@link DefaultPersistor}. Currently this is only used to determine
 * whether {@code SKIP LOCKED} is available, so using the wrong dialect may work for unsupported
 * database platforms. However, in future this is likely to extend to other SQL features and
 * possibly be expanded to an interface to allow easier extension.
 */
@AllArgsConstructor
@Getter
public enum Dialect {
  MY_SQL_5(false),
  MY_SQL_8(true),
  POSTGRESQL_9(true),
  H2(false);

  /**
   * @return True if hot row support ({@code SKIP LOCKED}) is available, increasing performance when
   *     there are multiple instances of the application potentially competing to process the same
   *     task.
   */
  @SuppressWarnings("JavaDoc")
  private final boolean supportsSkipLock;
}
