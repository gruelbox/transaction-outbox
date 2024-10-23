package com.gruelbox.transactionoutbox.performance.load;

import com.gruelbox.transactionoutbox.performance.TestDefaultPerformanceH2;
import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.domain.TestMappings;
import org.jsmart.zerocode.jupiter.extension.ParallelLoadExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@LoadWith("load_generation.properties")
@ExtendWith({ParallelLoadExtension.class})
public class LoadTestH2Test {
  @Test
  @DisplayName("Testing Parallel Load")
  @TestMappings({
    @TestMapping(testClass = TestDefaultPerformanceH2.class, testMethod = "testInsertAndSelect")
  })
  public void testLoad() {}
}
