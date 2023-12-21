package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.AbstractFullyQualifiedNameInstantiator;
import com.gruelbox.transactionoutbox.spi.Utils;
import java.lang.reflect.Constructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link Instantiator} which records the class name as its fully-qualified class name, and
 * instantiates via reflection. The class must have a no-args constructor. Likely only of use in
 * simple applications since it does not allow for dependency injection.
 */
@Slf4j
@SuperBuilder
final class ReflectionInstantiator extends AbstractFullyQualifiedNameInstantiator {

  @Override
  public Object createInstance(Class<?> clazz) {
    log.debug("Getting instance of class [{}] via reflection", clazz.getName());
    Constructor<?> constructor = Utils.uncheckedly(clazz::getDeclaredConstructor);
    constructor.setAccessible(true);
    return Utils.uncheckedly(constructor::newInstance);
  }
}
