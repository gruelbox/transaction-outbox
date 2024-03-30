package com.gruelbox.transactionoutbox.spi;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.Instantiator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract {@link Instantiator} implementation which simplifies the creation of implementations
 * which instantiate based on the clazz FQN.
 */
@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractFullyQualifiedNameInstantiator implements Instantiator {

  @Override
  public final String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  public final Object getInstance(String name) {
    log.debug("Getting class by name [{}]", name);
    return createInstance(uncheckedly(() -> Class.forName(name)));
  }

  protected abstract Object createInstance(Class<?> clazz);

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getInstance(Class<T> clazz) {
    return (T) createInstance(clazz);
  }
}
