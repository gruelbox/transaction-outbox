package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;

@SuppressWarnings("WeakerAccess")
class TestMySql5 extends AbstractAcceptanceTest {

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.MY_SQL_5)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url("jdbc:tc:mysql:5:///test?TC_REUSABLE=true&TC_TMPFS=/var/lib/mysql:rw")
        .user("test")
        .password("test")
        .build();
  }
}
