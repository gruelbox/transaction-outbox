package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.Dialect;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestPostgres10 extends AbstractAcceptanceTest {

    @Container
    @SuppressWarnings("rawtypes")
    private static final JdbcDatabaseContainer container =
            (JdbcDatabaseContainer)
                    new PostgreSQLContainer("postgres:10").withStartupTimeout(Duration.ofHours(1));

    @Override
    protected ConnectionDetails connectionDetails() {
        return ConnectionDetails.builder()
                .dialect(Dialect.POSTGRESQL_9)
                .driverClassName("org.postgresql.Driver")
                .url(container.getJdbcUrl())
                .user(container.getUsername())
                .password(container.getPassword())
                .build();
    }
}
