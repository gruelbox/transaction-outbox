package com.gruelbox.transactionoutbox.spring.acceptance;

import com.gruelbox.transactionoutbox.SchedulerProxyFactory;
import com.gruelbox.transactionoutbox.SpringTransaction;
import com.gruelbox.transactionoutbox.SpringTransactionManager;
import com.gruelbox.transactionoutbox.acceptance.AbstractJdbcAcceptanceTest;
import com.gruelbox.transactionoutbox.acceptance.JdbcConnectionDetails;
import com.gruelbox.transactionoutbox.sql.Dialects;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class TestSpringJdbcH2
    extends AbstractJdbcAcceptanceTest<SpringTransaction, SpringTransactionManager> {

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
  protected SpringTransactionManager createTxManager() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(ExternalsConfiguration.class);
    context.registerBean(DataSource.class, this::pooledDataSource);
    context.registerBean(
        PlatformTransactionManager.class,
        () -> new DataSourceTransactionManager(context.getBean(DataSource.class)));
    context.registerBean(
        TransactionTemplate.class,
        () -> new TransactionTemplate(context.getBean(PlatformTransactionManager.class)));
    context.refresh();
    return context.getBean(SpringTransactionManager.class);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithTx(
      SchedulerProxyFactory outbox, SpringTransaction tx, int arg1, String arg2) {
    return outbox.schedule(AsyncInterfaceProcessor.class).processAsync(arg1, arg2, tx);
  }

  @Override
  protected CompletableFuture<Void> scheduleWithCtx(
      SchedulerProxyFactory outbox, Object context, int arg1, String arg2) {
    throw new UnsupportedOperationException();
  }
}
