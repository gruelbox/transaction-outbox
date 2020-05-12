package com.gruelbox.transactionoutbox;

import com.sun.jna.platform.win32.COM.IPersist;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.validation.ValidationException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Testcontainers
class TestDefaultPersistorMySql5 extends AbstractDefaultPersistorTest {

    @Container
    @SuppressWarnings("rawtypes")
    private static final JdbcDatabaseContainer container =
        new MySQLContainer<>("mysql:5").withStartupTimeout(Duration.ofHours(1));

    private DefaultPersistor persistor = DefaultPersistor.builder().dialect(Dialect.MY_SQL_5).build();
    private TransactionManager txManager = TransactionManager.fromConnectionDetails(
        "com.mysql.cj.jdbc.Driver",
        container.getJdbcUrl(),
        container.getUsername(),
        container.getPassword()
    );

    @Override
    protected DefaultPersistor persistor() {
        return persistor;
    }

    @Override
    protected TransactionManager txManager() {
        return txManager;
    }
}
