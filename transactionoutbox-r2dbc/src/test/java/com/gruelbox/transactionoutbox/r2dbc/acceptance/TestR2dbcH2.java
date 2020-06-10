package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

class TestR2dbcH2 extends AbstractR2dbcAcceptanceTest {

  @Override
  protected Dialect dialect() {
    return Dialects.H2;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return new H2ConnectionFactory(
        H2ConnectionConfiguration.builder()
            .inMemory("test")
            .username("test")
            .password("test")
            .option("DB_CLOSE_DELAY=-1")
            .build());
  }
}
