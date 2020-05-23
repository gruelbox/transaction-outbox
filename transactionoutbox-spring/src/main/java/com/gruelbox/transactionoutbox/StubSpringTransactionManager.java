package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.StubThreadLocalJdbcTransactionManager;
import javax.persistence.EntityManager;

public class StubSpringTransactionManager
    extends StubThreadLocalJdbcTransactionManager<SpringTransaction>
    implements SpringTransactionManager {

  public StubSpringTransactionManager() {
    super(() -> new SpringTransaction(Utils.createLoggingProxy(EntityManager.class)));
  }
}
