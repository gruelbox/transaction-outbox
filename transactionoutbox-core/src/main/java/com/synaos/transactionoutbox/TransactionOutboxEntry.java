package com.synaos.transactionoutbox;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.Arrays;

import static java.util.stream.Collectors.joining;

/**
 * Internal representation of a {@link TransactionOutbox} task. Generally only directly of interest
 * to implementers of SPIs such as {@link Persistor} or {@link Submitter}.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class TransactionOutboxEntry implements Validatable, Comparable<TransactionOutboxEntry> {

    /**
     * @param id The id of the record. Usually a UUID.
     * @return The id of the record. Usually a UUID.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    private final String id;

    /**
     * @param uniqueRequestId A unique, client-supplied key for the entry. If supplied, it must be
     * globally unique
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    private final String uniqueRequestId;

    /**
     * @param invocation The method invocation to perform.
     * @return The method invocation to perform.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter(AccessLevel.PACKAGE)
    private Invocation invocation;

    /**
     * @param lastAttemptTime The timestamp at which the task was last processed.
     * @return The timestamp at which the task was last processed.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private Instant lastAttemptTime;

    /**
     * @param nextAttemptTime The timestamp after which the task is available for re-attempting.
     * @return The timestamp after which the task is available for re-attempting.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private Instant nextAttemptTime;

    /**
     * @param attempts The number of unsuccessful attempts so far made to run the task.
     * @return The number of unsuccessful attempts so far made to run the task.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private int attempts;

    /**
     * @param blocked True if the task has exceeded the configured maximum number of attempts.
     * @return True if the task has exceeded the configured maximum number of attempts.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private boolean blocked;

    /**
     * @param processed True if the task has been processed but has been retained to prevent duplicate
     * requests.
     * @return True if the task has been processed but has been retained to prevent * duplicate
     * requests.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private boolean processed;


    @Getter
    @Setter
    private Instant createdAt;

    @Getter
    @Setter
    private String groupId;

    /**
     * @param version The optimistic locking version. Monotonically increasing with each update.
     * @return The optimistic locking version. Monotonically increasing with each update.
     */
    @SuppressWarnings("JavaDoc")
    @Getter
    @Setter
    private int version;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private volatile boolean initialized;
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private String description;

    /**
     * @return A textual description of the task.
     */
    public String description() {
        if (!this.initialized) {
            synchronized (this) {
                if (!this.initialized) {
                    String description =
                            String.format(
                                    "%s.%s(%s) [%s]%s %s",
                                    invocation.getClassName(),
                                    invocation.getMethodName(),
                                    invocation.getArgs() == null
                                            ? null
                                            : Arrays.stream(invocation.getArgs())
                                            .map(this::stringify)
                                            .collect(joining(", ")),
                                    id,
                                    uniqueRequestId == null ? "" : " uid=[" + uniqueRequestId + "]",
                                    createdAt.toString());
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

    @Override
    public void validate(Validator validator) {
        validator.notNull("id", id);
        validator.nullOrNotBlank("uniqueRequestId", uniqueRequestId);
        validator.notNull("invocation", invocation);
        validator.inFuture("nextAttemptTime", nextAttemptTime);
        validator.positiveOrZero("attempts", attempts);
        validator.positiveOrZero("version", version);
        validator.notNull("createdAt", createdAt);
    }

    @Override
    public int compareTo(TransactionOutboxEntry otherEntry) {
        if (otherEntry == null) {
            throw new IllegalArgumentException("TransactionOutBoxEntry to compare can't be null");
        }
        return createdAt.compareTo(otherEntry.getCreatedAt());
    }
}
