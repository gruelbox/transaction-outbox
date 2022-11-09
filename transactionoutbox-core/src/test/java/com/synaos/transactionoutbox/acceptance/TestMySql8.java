package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.Dialect;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestMySql8 extends AbstractAcceptanceTest {

    @Container
    @SuppressWarnings("rawtypes")
    private static final JdbcDatabaseContainer container =
            new MySQLContainer<>("mysql:8.0.27").withStartupTimeout(Duration.ofMinutes(5));

    @Override
    protected ConnectionDetails connectionDetails() {
        return ConnectionDetails.builder()
                .dialect(Dialect.MY_SQL_8)
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .url(container.getJdbcUrl())
                .user(container.getUsername())
                .password(container.getPassword())
                .build();
    }
}
