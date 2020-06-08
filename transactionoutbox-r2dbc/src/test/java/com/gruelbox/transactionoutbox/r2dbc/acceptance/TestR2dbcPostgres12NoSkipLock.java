package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import static com.gruelbox.transactionoutbox.r2dbc.UsesPostgres12.connectionConfiguration;

import com.gruelbox.transactionoutbox.r2dbc.UsesPostgres12;
import com.gruelbox.transactionoutbox.sql.Dialect;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

class TestR2dbcPostgres12NoSkipLock extends AbstractR2dbcAcceptanceTest implements UsesPostgres12 {

  @SuppressWarnings("deprecation")
  @Override
  protected Dialect dialect() {
    return Dialect.POSTGRESQL__TEST_NO_SKIP_LOCK;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return new PostgresqlConnectionFactory(connectionConfiguration());
  }
}
