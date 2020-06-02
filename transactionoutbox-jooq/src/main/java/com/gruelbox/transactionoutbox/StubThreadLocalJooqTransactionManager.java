package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.StubThreadLocalJdbcTransactionManager;
import java.sql.Connection;
import org.jooq.Configuration;

public class StubThreadLocalJooqTransactionManager
    extends StubThreadLocalJdbcTransactionManager<Configuration, JooqTransaction>
    implements ThreadLocalJooqTransactionManager {
  public StubThreadLocalJooqTransactionManager() {
    super(() -> new JooqTransaction(Utils.createLoggingProxy(Connection.class), null));
  }
}
