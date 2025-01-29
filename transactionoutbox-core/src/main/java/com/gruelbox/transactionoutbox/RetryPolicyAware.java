package com.gruelbox.transactionoutbox;

import java.time.Duration;

/**
 * Defines a custom retry policy for tasks scheduled in the {@link TransactionOutbox}.
 * <p>
 * Implement this interface in the class that is passed to
 * {@link TransactionOutbox#schedule(Class)} to override the default retry behavior.
 * </p>
 */
public interface RetryPolicyAware {
  /**
   * Determines the wait duration before retrying a failed task.
   *
   * @param attempt   The current retry attempt (starting from 1).
   * @param throwable The exception that caused the failure.
   * @return The duration to wait before the next retry.
   */
  Duration waitDuration(int attempt, Throwable throwable);

  /**
   * Specifies the maximum number of retry attempts before blocking the task.
   *
   * @param attempt   The current retry attempt (starting from 1).
   * @param throwable The exception that caused the failure.
   * @return The number of attempts after which the task should be blocked.
   * If the returned value is less than or equal to {@code attempt}, the task is blocked immediately.
   */
  int blockAfterAttempts(int attempt, Throwable throwable);
}
