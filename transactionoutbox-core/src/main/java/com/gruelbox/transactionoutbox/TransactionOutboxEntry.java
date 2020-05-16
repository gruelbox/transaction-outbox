package com.gruelbox.transactionoutbox;

import static java.util.stream.Collectors.joining;

import java.time.Instant;
import java.util.Arrays;
import javax.validation.constraints.Future;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuppressWarnings("WeakerAccess")
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class TransactionOutboxEntry {

  @NotNull @Getter private final String id;

  @NotNull @Getter private final Invocation invocation;

  @Future @Getter @Setter private Instant nextAttemptTime;

  @PositiveOrZero @Getter @Setter private int attempts;

  @Getter @Setter private boolean blacklisted;

  @PositiveOrZero @Getter @Setter private int version;

  @EqualsAndHashCode.Exclude @ToString.Exclude private volatile boolean initialized;

  @EqualsAndHashCode.Exclude @ToString.Exclude private String description;

  public String description() {
    if (!this.initialized) {
      synchronized (this) {
        if (!this.initialized) {
          String description =
              String.format(
                  "%s.%s(%s) [%s]",
                  invocation.getClassName(),
                  invocation.getMethodName(),
                  Arrays.stream(invocation.getArgs()).map(this::stringify).collect(joining(", ")),
                  id);
          this.description = description;
          this.initialized = true;
          return description;
        }
      }
    }
    return this.description;
  }

  private String stringify(Object o) {
    if (o == null) {
      return "null";
    }
    if (o.getClass().isArray()) {
      return "[" + Arrays.stream((Object[]) o).map(this::stringify).collect(joining(", ")) + "]";
    }
    if (o instanceof String) {
      return "\"" + o + "\"";
    }
    return o.toString();
  }
}
