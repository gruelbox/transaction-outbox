package com.gruelbox.transactionoutbox.quarkus;

import com.gruelbox.transactionoutbox.spi.AbstractFullyQualifiedNameInstantiator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;

@ApplicationScoped
public class CdiInstantiator extends AbstractFullyQualifiedNameInstantiator {

  @SuppressWarnings("unused")
  public static CdiInstantiator create() {
    return new CdiInstantiator();
  }

  private CdiInstantiator() {}

  @Override
  protected Object createInstance(Class<?> clazz) {
    return CDI.current().select(clazz).get();
  }
}
