package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import static com.gruelbox.transactionoutbox.r2dbc.UsesMySql5.connectionConfiguration;

import com.gruelbox.transactionoutbox.r2dbc.UsesMySql5;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

class TestR2dbcMySql5 extends AbstractR2dbcAcceptanceTest implements UsesMySql5 {

  @Override
  protected Dialect dialect() {
    return Dialects.MY_SQL_5;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return MySqlConnectionFactory.from(connectionConfiguration());
  }
}
