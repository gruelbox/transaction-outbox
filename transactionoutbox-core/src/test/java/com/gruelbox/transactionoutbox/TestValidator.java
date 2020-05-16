package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import javax.validation.ClockProvider;
import javax.validation.ValidationException;
import org.junit.jupiter.api.Test;

class TestValidator {

  private static final Invocation COMPLEX_INVOCATION =
      new Invocation(
          "Foo",
          "Bar",
          new Class<?>[] {int.class, BigDecimal.class, String.class},
          new Object[] {1, BigDecimal.TEN, null});

  private final ClockProvider clockProvider = () -> Clock.fixed(Instant.now(), ZoneId.of("+4"));
  private final Validator validator = new Validator(clockProvider);

  @Test
  void testEntryDateInPast() {
    TransactionOutboxEntry entry =
        TransactionOutboxEntry.builder()
            .id("FOO")
            .invocation(COMPLEX_INVOCATION)
            .nextAttemptTime(clockProvider.getClock().instant().minusMillis(1))
            .build();
    assertThrows(ValidationException.class, () -> validator.validate(entry));
  }

  @Test
  void testEntryDateNow() {
    TransactionOutboxEntry entry =
        TransactionOutboxEntry.builder()
            .id("FOO")
            .invocation(COMPLEX_INVOCATION)
            .nextAttemptTime(clockProvider.getClock().instant())
            .build();
    assertThrows(ValidationException.class, () -> validator.validate(entry));
  }

  @Test
  void testEntryDateFuture() {
    TransactionOutboxEntry entry =
        TransactionOutboxEntry.builder()
            .id("FOO")
            .invocation(COMPLEX_INVOCATION)
            .nextAttemptTime(clockProvider.getClock().instant().plusMillis(1))
            .build();
    assertThrows(ValidationException.class, () -> validator.validate(entry));
  }
}
