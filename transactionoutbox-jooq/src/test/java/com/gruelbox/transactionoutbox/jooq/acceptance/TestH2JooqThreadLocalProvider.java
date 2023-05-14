package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.JooqTransactionListener;
import com.gruelbox.transactionoutbox.JooqTransactionManager;
import com.gruelbox.transactionoutbox.ThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.acceptance.JdbcConnectionDetails;
import com.gruelbox.transactionoutbox.sql.Dialects;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;

public class TestH2JooqThreadLocalProvider extends AbstractJooqThreadLocalProviderTest {

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialects.H2)
        .driverClassName("org.h2.Driver")
        .url(
            "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE")
        .user("test")
        .password("test")
        .build();
  }

  @Override
  protected ThreadLocalJooqTransactionManager createTxManager() {
    DataSourceConnectionProvider connectionProvider =
        new DataSourceConnectionProvider(pooledDataSource());
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.setConnectionProvider(connectionProvider);
    configuration.setSQLDialect(SQLDialect.H2);
    configuration.setTransactionProvider(
        new ThreadLocalTransactionProvider(connectionProvider, true));
    JooqTransactionListener listener = JooqTransactionManager.createListener();
    configuration.set(listener);
    dsl = DSL.using(configuration);
    return JooqTransactionManager.create(dsl, listener);
  }
}
