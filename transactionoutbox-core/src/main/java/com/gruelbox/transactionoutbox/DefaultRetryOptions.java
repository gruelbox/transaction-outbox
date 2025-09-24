package com.gruelbox.transactionoutbox;

import java.time.Duration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultRetryOptions implements NextRetryStrategy.Options {

  Duration attemptFrequency;

  @Override
  public String strategyClassName() {
    return DefaultRetryStrategy.class.getName();
  }

  public static DefaultRetryOptions withFrequency(Duration attemptFrequency) {
    return new DefaultRetryOptions(attemptFrequency);
  }
}
