package com.gruelbox.transactionoutbox.guice;

import com.google.inject.Injector;
import com.gruelbox.transactionoutbox.spi.AbstractFullyQualifiedNameInstantiator;
import lombok.experimental.SuperBuilder;

/** Instantiator that uses the Guice {@link Injector} to source objects. */
@SuperBuilder
public class GuiceInstantiator extends AbstractFullyQualifiedNameInstantiator {

  private final Injector injector;

  @Override
  protected Object createInstance(Class<?> clazz) {
    return injector.getInstance(clazz);
  }
}
