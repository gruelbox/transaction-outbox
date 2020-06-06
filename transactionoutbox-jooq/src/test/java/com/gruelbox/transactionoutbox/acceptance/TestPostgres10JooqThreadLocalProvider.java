package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.JooqTransactionListener;
import com.gruelbox.transactionoutbox.JooqTransactionManager;
import com.gruelbox.transactionoutbox.ThreadLocalJooqTransactionManager;
import com.gruelbox.transactionoutbox.sql.Dialect;
import java.time.Duration;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestPostgres10JooqThreadLocalProvider extends AbstractJooqThreadLocalProviderTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer<>("postgres:10").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected JdbcConnectionDetails connectionDetails() {
    return JdbcConnectionDetails.builder()
        .dialect(Dialect.POSTGRESQL_9)
        .driverClassName("org.postgresql.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }

  @Override
  protected ThreadLocalJooqTransactionManager createTxManager() {
    DataSourceConnectionProvider connectionProvider =
        new DataSourceConnectionProvider(pooledDataSource());
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.setConnectionProvider(connectionProvider);
    configuration.setSQLDialect(SQLDialect.POSTGRES);
    configuration.setTransactionProvider(
        new ThreadLocalTransactionProvider(connectionProvider, true));
    JooqTransactionListener listener = JooqTransactionManager.createListener();
    configuration.set(listener);
    dsl = DSL.using(configuration);
    return JooqTransactionManager.create(dsl, listener);
  }
}
