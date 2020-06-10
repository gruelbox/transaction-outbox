package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import static com.gruelbox.transactionoutbox.r2dbc.UsesPostgres12.connectionConfiguration;

import com.gruelbox.transactionoutbox.r2dbc.UsesPostgres12;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

class TestR2dbcPostgres12 extends AbstractR2dbcAcceptanceTest implements UsesPostgres12 {

  @Override
  protected Dialect dialect() {
    return Dialects.POSTGRESQL_9;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return new PostgresqlConnectionFactory(connectionConfiguration());
  }
}
