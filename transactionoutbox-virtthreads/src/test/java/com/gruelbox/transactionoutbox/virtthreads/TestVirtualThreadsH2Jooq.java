package com.gruelbox.transactionoutbox.virtthreads;

import com.gruelbox.transactionoutbox.ThreadLocalContextTransactionManager;
import com.gruelbox.transactionoutbox.jooq.JooqTransactionListener;
import com.gruelbox.transactionoutbox.jooq.JooqTransactionManager;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;
import org.junit.jupiter.api.BeforeEach;

public class TestVirtualThreadsH2Jooq extends AbstractVirtualThreadsTest {

  private ThreadLocalContextTransactionManager txm;

  @Override
  protected final ThreadLocalContextTransactionManager txManager() {
    return txm;
  }

  @BeforeEach
  final void beforeEach() {
    DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.setConnectionProvider(connectionProvider);
    configuration.setSQLDialect(SQLDialect.H2);
    configuration.setTransactionProvider(
        new ThreadLocalTransactionProvider(connectionProvider, true));
    JooqTransactionListener listener = JooqTransactionManager.createListener();
    configuration.set(listener);
    txm = JooqTransactionManager.create(DSL.using(configuration), listener);
  }
}
