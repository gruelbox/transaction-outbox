package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Instantiator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * Instantiator that uses the spring {@link ApplicationContext} to source objects. It requires that
 * classes scheduled have a unique name in the context, so doesn't often play well with proxies and
 * other auto-generated code such as repositories based on {@code CrudRepository}.
 */
@Service
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
