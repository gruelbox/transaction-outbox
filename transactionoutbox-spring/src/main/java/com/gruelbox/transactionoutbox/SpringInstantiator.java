package com.gruelbox.transactionoutbox;

import java.util.Arrays;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/**
 * @deprecated use {@link com.gruelbox.transactionoutbox.spring.SpringInstantiator}.
 */
@Service
@Deprecated(forRemoval = true)
public class SpringInstantiator implements Instantiator {

  private final ApplicationContext applicationContext;

  @Autowired
  public SpringInstantiator(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Override
  public String getName(Class<?> clazz) {
    String[] beanNames = applicationContext.getBeanNamesForType(clazz);
    if (beanNames.length > 1) {
      throw new IllegalArgumentException(
          "Type "
              + clazz.getName()
              + " exists under multiple names in the context: "
              + Arrays.toString(beanNames)
              + ". Use a unique type.");
    }
    if (beanNames.length == 0) {
      throw new IllegalArgumentException(
          "Type " + clazz.getName() + " not available in the context");
    }
    return beanNames[0];
  }

  @Override
  public Object getInstance(String name) {
    return applicationContext.getBean(name);
  }
}
