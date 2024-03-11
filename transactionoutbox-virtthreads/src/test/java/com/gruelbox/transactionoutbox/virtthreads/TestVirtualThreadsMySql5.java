package com.gruelbox.transactionoutbox.virtthreads;


import com.gruelbox.transactionoutbox.Dialect;
import org.junit.jupiter.api.Disabled;

@SuppressWarnings("WeakerAccess")
@Disabled
class TestVirtualThreadsMySql5 extends AbstractVirtualThreadsTest {

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
