package com.synaos.transactionoutbox;

import com.google.inject.Injector;
import lombok.experimental.SuperBuilder;

/** Instantiator that uses the Guice {@link com.google.inject.Injector} to source objects. */
@SuperBuilder
public class GuiceInstantiator extends AbstractFullyQualifiedNameInstantiator {

  private final Injector injector;

  @Override
  protected Object createInstance(Class<?> clazz) {
    return injector.getInstance(clazz);
  }
}
