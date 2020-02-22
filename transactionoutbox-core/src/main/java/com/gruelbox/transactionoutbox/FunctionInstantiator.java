package com.gruelbox.transactionoutbox;

import java.util.function.Function;
import lombok.experimental.SuperBuilder;

@SuperBuilder
class FunctionInstantiator extends AbstractFullyQualifiedNameInstantiator {

  private final Function<Class<?>, Object> fn;

  @Override
  public Object createInstance(Class<?> clazz) {
    return fn.apply(clazz);
  }
}
