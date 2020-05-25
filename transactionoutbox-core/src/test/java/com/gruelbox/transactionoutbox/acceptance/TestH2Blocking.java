package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Dialect;

@SuppressWarnings("WeakerAccess")
class TestH2Blocking extends AbstractBlockingAcceptanceTest {

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.H2)
        .driverClassName("org.h2.Driver")
        .url(
            "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE")
        .user("test")
        .password("test")
        .build();
  }
}
