package com.gruelbox.transactionoutbox.performance.load;

import com.gruelbox.transactionoutbox.performance.ContainerUtils;
import com.gruelbox.transactionoutbox.performance.TestDefaultPerformancePostgresql16;
import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.domain.TestMappings;
import org.jsmart.zerocode.jupiter.extension.ParallelLoadExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.JdbcDatabaseContainer;

@LoadWith("load_generation.properties")
@ExtendWith({ParallelLoadExtension.class})
public class LoadTestPostgresql16Test {
  @SuppressWarnings({"rawtypes"})
  private static final JdbcDatabaseContainer container = ContainerUtils.getPostgres16Container();

  @Test
  @DisplayName("Testing Parallel Load")
  @TestMappings({
    @TestMapping(
        testClass = TestDefaultPerformancePostgresql16.class,
        testMethod = "testInsertAndSelect")
  })
  public void testLoad() {}

  @BeforeAll
  public static void beforeAll() {
    container.start();
  }

  @AfterAll
  public static void afterAll() {
    container.stop();
  }
}
