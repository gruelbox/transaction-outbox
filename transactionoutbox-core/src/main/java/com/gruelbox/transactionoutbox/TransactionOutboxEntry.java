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

/**
 * Internal representation of a {@link TransactionOutbox} task. Generally only directly of interest
 * to implementers of SPIs such as {@link Persistor} or {@link Submitter}.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class TransactionOutboxEntry {

  /**
   * @param id The id of the record. Usually a UUID.
   * @return The id of the record. Usually a UUID.
   */
  @SuppressWarnings("JavaDoc")
  @NotNull
  @Getter
  private final String id;

  /**
   * @param invocation The method invocation to perform.
   * @return The method invocation to perform.
   */
  @SuppressWarnings("JavaDoc")
  @NotNull
  @Getter
  private final Invocation invocation;

  /**
   * @param nextAttemptTime The timestamp after which the task is available for re-attempting.
   * @return The timestamp after which the task is available for re-attempting.
   */
  @SuppressWarnings("JavaDoc")
  @Future
  @Getter
  @Setter
  private Instant nextAttemptTime;

  /**
   * @param attempts The number of unsuccessful attempts so far made to run the task.
   * @return The number of unsuccessful attempts so far made to run the task.
   */
  @SuppressWarnings("JavaDoc")
  @PositiveOrZero
  @Getter
  @Setter
  private int attempts;

  /**
   * @param blacklisted True if the task has exceeded the configured maximum number of attempts.
   * @return True if the task has exceeded the configured maximum number of attempts.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private boolean blacklisted;

  /**
   * @param version The optimistic locking version. Monotonically increasing with each update.
   * @return The optimistic locking version. Monotonically increasing with each update.
   */
  @SuppressWarnings("JavaDoc")
  @PositiveOrZero
  @Getter
  @Setter
  private int version;

  @EqualsAndHashCode.Exclude @ToString.Exclude private volatile boolean initialized;
  @EqualsAndHashCode.Exclude @ToString.Exclude private String description;

  /** @return A textual description of the task. */
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
