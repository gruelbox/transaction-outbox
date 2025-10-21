package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Nested;

@Slf4j
class TestDefaultInvocationSerializer {

  @Nested
  class Version1 extends AbstractTestDefaultInvocationSerializer {
    public Version1() {
      super(1);
    }

    @Override
    void testSession() {
      // Not supported
    }
  }

  @Nested
  class Version2 extends AbstractTestDefaultInvocationSerializer {
    public Version2() {
      super(2);
    }
  }

  @Nested
  class NullVersion extends AbstractTestDefaultInvocationSerializer {
    public NullVersion() {
      super(null);
    }
  }
}
