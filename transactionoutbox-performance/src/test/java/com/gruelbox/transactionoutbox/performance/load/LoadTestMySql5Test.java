package com.gruelbox.transactionoutbox.performance.load;

import com.gruelbox.transactionoutbox.performance.ContainerUtils;
import com.gruelbox.transactionoutbox.performance.TestDefaultPerformanceMySql5;
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
public class LoadTestMySql5Test {
  @SuppressWarnings({"rawtypes"})
  private static final JdbcDatabaseContainer container = ContainerUtils.getMySql5Container();

  @Test
  @DisplayName("Testing Parallel Load")
  @TestMappings({
    @TestMapping(testClass = TestDefaultPerformanceMySql5.class, testMethod = "testInsertAndSelect")
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
