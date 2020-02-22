package com.gruelbox.transactionoutbox;

import lombok.experimental.SuperBuilder;
import org.springframework.context.ApplicationContext;

/** Instantiator that uses the spring {@link ApplicationContext} to source objects. */
@SuperBuilder
public class SpringInstantiator extends AbstractFullyQualifiedNameInstantiator {

  private final ApplicationContext applicationContext;

  @Override
  protected Object createInstance(Class<?> clazz) {
    return applicationContext.getBean(clazz);
  }
}
