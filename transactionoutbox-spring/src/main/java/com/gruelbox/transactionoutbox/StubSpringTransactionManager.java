package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import com.gruelbox.transactionoutbox.jdbc.StubThreadLocalJdbcTransactionManager;
import java.sql.Connection;

/** Stub implementation of {@link SpringTransactionManager}, for use in tests. */
public class StubSpringTransactionManager
    extends StubThreadLocalJdbcTransactionManager<SpringTransaction>
    implements SpringTransactionManager {

  public StubSpringTransactionManager() {
    super(StubSpringTransaction::new);
  }

  private static final class StubSpringTransaction extends SimpleTransaction<Void>
      implements SpringTransaction {

    StubSpringTransaction() {
      super(Utils.createLoggingProxy(new ProxyFactory(), Connection.class), null);
    }
  }
}
