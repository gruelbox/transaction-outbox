package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.AbstractFullyQualifiedNameInstantiator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;

/**
 * @deprecated use {@link com.gruelbox.transactionoutbox.quarkus.CdiInstantiator}.
 */
@ApplicationScoped
@Deprecated(forRemoval = true)
public class CdiInstantiator extends AbstractFullyQualifiedNameInstantiator {

  public static CdiInstantiator create() {
    return new CdiInstantiator();
  }

  private CdiInstantiator() {}

  @Override
  protected Object createInstance(Class<?> clazz) {
    return CDI.current().select(clazz).get();
  }
}
