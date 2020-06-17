package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Beta;
import java.util.function.Supplier;

/** Stub implementation of {@link SimpleTransactionManager}. */
public class StubSimpleTransactionManager
    extends StubThreadLocalJdbcTransactionManager<SimpleTransaction<Void>>
    implements SimpleTransactionManager {

  @Beta
  public StubSimpleTransactionManager(Supplier<SimpleTransaction<Void>> transactionFactory) {
    super(transactionFactory);
  }
}
