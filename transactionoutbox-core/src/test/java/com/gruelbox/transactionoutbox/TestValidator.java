package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class TestValidator {

  private static final Invocation COMPLEX_INVOCATION =
      new Invocation(
          "Foo",
          "Bar",
          new Class<?>[] {int.class, BigDecimal.class, String.class},
          new Object[] {1, BigDecimal.TEN, null});

  private final Instant now = Instant.now();
  private final Validator validator = new Validator(() -> Clock.fixed(now, ZoneId.of("+4")));

  @Test
  void testEntryDateFuture() {
    TransactionOutboxEntry entry =
        TransactionOutboxEntry.builder()
            .id("FOO")
            .invocation(COMPLEX_INVOCATION)
            .nextAttemptTime(now.plusMillis(1))
            .build();
    assertDoesNotThrow(() -> validator.validate(entry));
  }
}
