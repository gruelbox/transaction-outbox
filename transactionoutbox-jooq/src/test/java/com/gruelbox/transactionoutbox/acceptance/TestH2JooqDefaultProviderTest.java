package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.DefaultJooqTransactionManager;
import com.gruelbox.transactionoutbox.JooqTransactionManager;
import com.gruelbox.transactionoutbox.sql.Dialect;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class TestH2JooqDefaultProviderTest extends AbstractJooqDefaultProviderTest {

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialect.H2)
        .driverClassName("org.h2.Driver")
        .url(
            "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE")
        .user("test")
        .password("test")
        .build();
  }

  @Override
  protected DefaultJooqTransactionManager createTxManager() {
    dsl = DSL.using(pooledDataSource(), SQLDialect.H2);
    dsl.configuration().set(JooqTransactionManager.createListener());
    return JooqTransactionManager.create(dsl);
  }
}
