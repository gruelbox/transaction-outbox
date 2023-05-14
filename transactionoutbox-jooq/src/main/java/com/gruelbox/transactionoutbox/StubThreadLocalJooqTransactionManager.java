package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.StubThreadLocalJdbcTransactionManager;
import java.sql.Connection;

public class StubThreadLocalJooqTransactionManager
    extends StubThreadLocalJdbcTransactionManager<JooqTransaction>
    implements ThreadLocalJooqTransactionManager {
  public StubThreadLocalJooqTransactionManager() {
    super(
        () ->
            new JooqTransaction(
                Utils.createLoggingProxy(new ProxyFactory(), Connection.class), null));
  }
}
