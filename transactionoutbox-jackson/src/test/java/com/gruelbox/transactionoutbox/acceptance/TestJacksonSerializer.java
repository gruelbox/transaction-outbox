package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestJacksonSerializer {

  private HikariDataSource dataSource;
  private ThreadLocalContextTransactionManager transactionManager;
  private DefaultPersistor persistor;
  private TransactionOutbox outbox;
  private final CountDownLatch latch = new CountDownLatch(1);

  @BeforeEach
  void beforeEach() {
    dataSource = pooledDataSource();
    transactionManager =
        TransactionManager.fromConnectionDetails(
            "org.h2.Driver",
            "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
            "test",
            "test");
    var mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    persistor =
        DefaultPersistor.builder()
            .dialect(new DialectSqlH2Impl())
            .serializer(JacksonInvocationSerializer.builder().mapper(mapper).build())
            .build();
    outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(persistor)
            .instantiator(Instantiator.using(clazz -> TestJacksonSerializer.this))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry) {
                    latch.countDown();
                  }
                })
            .build();
    transactionManager.inTransaction(
        tx -> {
          try {
            persistor.clear(tx);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @AfterEach
  void afterEach() {
    dataSource.close();
  }

  private HikariDataSource pooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE");
    config.setUsername("test");
    config.setPassword("test");
    config.addDataSourceProperty("cachePrepStmts", "true");
    return new HikariDataSource(config);
  }

  void process(List<Object> difficultDataStructure) {
    assertEquals(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2), difficultDataStructure);
  }

  @Test
  void testPolymorphicDeserialization() throws InterruptedException {
    transactionManager.inTransaction(
        () -> outbox.schedule(getClass()).process(List.of(LocalDate.of(2000, 1, 1), "a", "b", 2)));
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }
}
