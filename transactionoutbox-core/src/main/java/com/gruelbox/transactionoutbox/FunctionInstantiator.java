package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.AbstractFullyQualifiedNameInstantiator;
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
