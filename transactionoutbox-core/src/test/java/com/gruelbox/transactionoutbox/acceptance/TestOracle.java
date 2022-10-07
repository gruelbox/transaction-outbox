package com.gruelbox.transactionoutbox.acceptance;

import java.time.Duration;

import com.gruelbox.transactionoutbox.Dialect;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestOracle extends AbstractAcceptanceTest
{

   @Container
   @SuppressWarnings("rawtypes")
   private static final JdbcDatabaseContainer container = (JdbcDatabaseContainer) new OracleContainer("gvenzl/oracle-xe:18-slim").withStartupTimeout(Duration.ofHours(1));

   @Override
   protected ConnectionDetails connectionDetails()
   {
      return ConnectionDetails.builder().dialect(Dialect.ORACLE).driverClassName("org.postgresql.Driver").url(container.getJdbcUrl()).user(container.getUsername())
         .password(container.getPassword()).build();
   }
}
