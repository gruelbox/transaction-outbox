package com.gruelbox.transactionoutbox;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class LambdaContext {
  private final Instantiator instantiator;

  public <T> T getInstance(Class<T> clazz) {
    return instantiator.getInstance(clazz);
  }
}
