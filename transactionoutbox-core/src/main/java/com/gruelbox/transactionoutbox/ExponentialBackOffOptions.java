package com.gruelbox.transactionoutbox;

import java.time.Duration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ExponentialBackOffOptions implements NextRetryStrategy.Options {

  Duration attemptFrequency;
  int backOff;

  public static ExponentialBackOffOptions exponential(Duration attemptFrequency, int backOff) {
    return new ExponentialBackOffOptions(attemptFrequency, backOff);
  }

  @Override
  public String strategyClassName() {
    return ExponentialBackOffStrategy.class.getName();
  }
}
