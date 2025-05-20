package com.gruelbox.transactionoutbox.testing;

import com.gruelbox.transactionoutbox.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public abstract class BaseTest {

  protected HikariDataSource dataSource;
  private ExecutorService flushExecutor;

  @BeforeEach
  final void baseBeforeEach() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(connectionDetails().url());
    config.setUsername(connectionDetails().user());
    config.setPassword(connectionDetails().password());
    config.addDataSourceProperty("cachePrepStmts", "true");
    dataSource = new HikariDataSource(config);
    flushExecutor = Executors.newFixedThreadPool(4);
  }

  @AfterEach
  final void baseAfterEach() throws InterruptedException {
    flushExecutor.shutdown();
    Assertions.assertTrue(flushExecutor.awaitTermination(30, TimeUnit.SECONDS));
    dataSource.close();
  }

  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(Dialect.H2)
        .driverClassName("org.h2.Driver")
        .url(
            "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE;DATABASE_TO_UPPER=FALSE")
        .user("test")
        .password("test")
        .build();
  }

  protected TransactionManager txManager() {
    return TransactionManager.fromDataSource(dataSource);
  }

  protected Persistor persistor() {
    return Persistor.forDialect(connectionDetails().dialect());
  }

  protected void clearOutbox() {
    DefaultPersistor persistor = Persistor.forDialect(connectionDetails().dialect());
    TransactionManager transactionManager = txManager();
    transactionManager.inTransaction(
        tx -> {
          try {
            persistor.clear(tx);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  protected void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    withRunningFlusher(outbox, runnable, flushExecutor);
  }

  protected void withRunningFlusher(
      TransactionOutbox outbox, ThrowingRunnable runnable, Executor executor) throws Exception {
    withRunningFlusher(outbox, runnable, executor, null);
  }

  protected void withRunningFlusher(
      TransactionOutbox outbox, ThrowingRunnable runnable, Executor executor, String topicName)
      throws Exception {
    Thread backgroundThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  // Keep flushing work until there's nothing left to flush
                  log.info("Starting flush...");
                  while (topicName == null
                      ? outbox.flush(executor)
                      : outbox.flushTopics(executor, topicName)) {
                    log.info("More work to do...");
                  }
                  log.info("Done!");
                } catch (Exception e) {
                  log.error("Error flushing transaction outbox", e);
                }
                try {
                  //noinspection BusyWait
                  Thread.sleep(250);
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
    backgroundThread.start();
    try {
      runnable.run();
    } finally {
      backgroundThread.interrupt();
      backgroundThread.join();
    }
  }

  @Value
  @Accessors(fluent = true)
  @Builder
  public static class ConnectionDetails {
    String driverClassName;
    String url;
    String user;
    String password;
    Dialect dialect;
  }
}
