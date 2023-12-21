package com.gruelbox.transactionoutbox;

import static com.gruelbox.transactionoutbox.Dialect.H2;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestDefaultMigrationManager {

  private static HikariDataSource dataSource;

  @BeforeAll
  static void beforeAll() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE;DATABASE_TO_UPPER=FALSE");
    config.setUsername("test");
    config.setPassword("test");
    config.addDataSourceProperty("cachePrepStmts", "true");
    dataSource = new HikariDataSource(config);
  }

  @AfterAll
  static void afterAll() {
    dataSource.close();
  }

  @Test
  void parallelMigrations() {
    CountDownLatch readyLatch = new CountDownLatch(2);
    DefaultMigrationManager.withLatch(
        readyLatch,
        waitLatch -> {
          Executor executor = runnable -> new Thread(runnable).start();
          TransactionManager txm = TransactionManager.fromDataSource(dataSource);
          CompletableFuture<?> threads =
              CompletableFuture.allOf(
                  CompletableFuture.runAsync(
                      () -> {
                        try {
                          DefaultMigrationManager.migrate(txm, H2);
                        } catch (Exception e) {
                          log.error("Thread 1 failed", e);
                          throw e;
                        }
                      },
                      executor),
                  CompletableFuture.runAsync(
                      () -> {
                        try {
                          DefaultMigrationManager.migrate(txm, H2);
                        } catch (Exception e) {
                          log.error("Thread 2 failed", e);
                          throw e;
                        }
                      },
                      executor));
          try {
            if (!readyLatch.await(15, TimeUnit.SECONDS)) {
              throw new TimeoutException();
            }
            waitLatch.countDown();
          } catch (InterruptedException | TimeoutException e) {
            fail("Timed out or interrupted waiting for ready latch");
          } finally {
            threads.join();
          }
        });
  }
}
