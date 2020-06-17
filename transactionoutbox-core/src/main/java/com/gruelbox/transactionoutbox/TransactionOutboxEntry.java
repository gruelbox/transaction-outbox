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

/** Internal representation of a {@code TransactionOutbox} task. */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
public final class TransactionOutboxEntry {

  /**
   * @param id The id of the record. Usually a UUID.
   * @return The id of the record. Usually a UUID.
   */
  @SuppressWarnings("JavaDoc")
  @NotNull
  @Getter
  private final String id;

  /**
   * @param uniqueRequestId A unique, client-supplied key for the entry. If supplied, it must be
   *     globally unique.
   * @return A unique, client-supplied key for the entry. If supplied, it must be globally unique.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  private final String uniqueRequestId;

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
   * @param processed True if the task has been processed but has been retained to prevent duplicate
   *     requests.
   * @return True if the task has been processed but has been retained to prevent * duplicate
   *     requests.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private boolean processed;

  /**
   * @param version The optimistic locking version. Monotonically increasing with each update.
   * @return The optimistic locking version. Monotonically increasing with each update.
   */
  @SuppressWarnings("JavaDoc")
  @PositiveOrZero
  @Getter
  @Setter
  private int version;

  @EqualsAndHashCode.Exclude @ToString.Exclude private transient volatile boolean initialized;
  @EqualsAndHashCode.Exclude @ToString.Exclude private transient String description;

  /** @return A textual description of the task. */
  public String description() {
    if (!this.initialized) {
      synchronized (this) {
        if (!this.initialized) {
          String description =
              String.format(
                  "%s.%s(%s) [%s]%s",
                  invocation.getClassName(),
                  invocation.getMethodName(),
                  invocation.getArgs() == null
                      ? ""
                      : Arrays.stream(invocation.getArgs())
                          .map(this::stringify)
                          .collect(joining(", ")),
                  id,
                  uniqueRequestId == null ? "" : " uid=[" + uniqueRequestId + "]");
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
