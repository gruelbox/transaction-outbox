package com.gruelbox.transactionoutbox;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
final class LambdaRunner {

  private final LambdaContext lambdaContext;

  void run(SerializableLambda runnable) {
    runnable.run(lambdaContext);
  }
}
