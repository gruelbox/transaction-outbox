package com.gruelbox.transactionoutbox;

import java.time.Duration;

public interface RetryPolicyAware {
  Duration waitDuration(int attempt, Throwable throwable);

  int blockAfterAttempts(int attempt, Throwable throwable);
}
