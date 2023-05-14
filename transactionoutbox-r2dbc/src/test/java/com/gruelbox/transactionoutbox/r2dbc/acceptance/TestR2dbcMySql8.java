package com.gruelbox.transactionoutbox.r2dbc.acceptance;

import com.gruelbox.transactionoutbox.r2dbc.UsesMySql8;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import dev.miku.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

class TestR2dbcMySql8 extends AbstractR2dbcAcceptanceTest implements UsesMySql8 {

  @Override
  protected Dialect dialect() {
    return Dialects.MY_SQL_8;
  }

  @Override
  protected ConnectionFactory createConnectionFactory() {
    return MySqlConnectionFactory.from(UsesMySql8.connectionConfiguration());
  }
}
