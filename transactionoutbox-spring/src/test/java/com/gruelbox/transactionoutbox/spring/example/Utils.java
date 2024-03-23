package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.ThrowingRunnable;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  @SuppressWarnings({"SameParameterValue", "WeakerAccess", "UnusedReturnValue"})
  static boolean safelyRun(String gerund, ThrowingRunnable runnable) {
    try {
      runnable.run();
      return true;
    } catch (Exception e) {
      LOGGER.error("Error when {}", gerund, e);
      return false;
    }
  }

  @SuppressWarnings("unused")
  static void safelyClose(AutoCloseable... closeables) {
    safelyClose(Arrays.asList(closeables));
  }

  private static void safelyClose(Iterable<? extends AutoCloseable> closeables) {
    closeables.forEach(
        d -> {
          if (d == null) return;
          safelyRun("closing resource", d::close);
        });
  }
}
