package com.gruelbox.transactionoutbox.spring.acceptance;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.SchedulerProxyFactory;
import com.gruelbox.transactionoutbox.SpringR2dbcTransaction;
import com.gruelbox.transactionoutbox.SpringR2dbcTransactionManager;
import com.gruelbox.transactionoutbox.SpringTransaction;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.acceptance.AbstractJdbcAcceptanceTest;
import com.gruelbox.transactionoutbox.acceptance.AbstractR2dbcAcceptanceTest;
import com.gruelbox.transactionoutbox.acceptance.AbstractSqlAcceptanceTest;
import com.gruelbox.transactionoutbox.acceptance.AsyncInterfaceProcessor;
import com.gruelbox.transactionoutbox.acceptance.JdbcConnectionDetails;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcPersistor;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.Dialects;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;

public class TestSpringR2dbcH2 extends AbstractSqlAcceptanceTest<Connection, SpringR2dbcTransaction, SpringR2dbcTransactionManager> {

  @Override
  protected Dialect dialect() {
    return Dialects.H2;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected Persistor<Connection, SpringR2dbcTransaction> createPersistor() {
    return (Persistor) R2dbcPersistor.forDialect(dialect());
  }

  @Override
  protected SpringR2dbcTransactionManager createTxManager() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(ExternalsR2dbcConfiguration.class);
    context.refresh();
    return context.getBean(SpringR2dbcTransactionManager.class);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
    SchedulerProxyFactory outbox, SpringR2dbcTransaction tx, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
    SchedulerProxyFactory outbox, Object context, int arg1, String arg2) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CompletableFuture<?> runSql(Object txOrContext, String sql) {
    return null;
  }

  @Override
  protected CompletableFuture<Long> readLongValue(SpringR2dbcTransaction springR2dbcTransaction, String sql) {
    return null;
  }
}
