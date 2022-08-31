package com.gruelbox.transactionoutbox;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.CDI;

@ApplicationScoped
public class CdiInstanciator extends AbstractFullyQualifiedNameInstantiator
{

   @Override
   protected Object createInstance(Class<?> clazz)
   {
      return CDI.current().select(clazz).get();
   }
}
