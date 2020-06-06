package com.gruelbox.transactionoutbox.acceptance.v1;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.acceptance.ConnectionDetails;

class TestH2 extends AbstractAcceptanceTestV1 {

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
