package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.*;
import lombok.extern.slf4j.Slf4j;
import org.jooq.SQLDialect;

@Slf4j
class TestJooqThreadLocalMySql8 extends AbstractJooqAcceptanceThreadLocalTest {

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.MY_SQL_8)
        .driverClassName("com.mysql.cj.jdbc.Driver")
        .url("jdbc:tc:mysql:8:///test?TC_REUSABLE=true&TC_TMPFS=/var/lib/mysql:rw")
        .user("test")
        .password("test")
        .build();
  }

  @Override
  protected SQLDialect jooqDialect() {
    return SQLDialect.MYSQL;
  }
}
