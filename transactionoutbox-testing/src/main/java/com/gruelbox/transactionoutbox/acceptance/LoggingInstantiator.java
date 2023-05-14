package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.ProxyFactory;
import com.gruelbox.transactionoutbox.Utils;
import java.lang.reflect.Method;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingInstantiator implements Instantiator {

  public static final ProxyFactory PROXY_FACTORY = new ProxyFactory();

  @Override
  public String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  @SneakyThrows
  public Object getInstance(String name) {
    return PROXY_FACTORY.createProxy(Class.forName(name), this::invoke);
  }

  @SuppressWarnings("unchecked")
  private <T> T invoke(Method method, Object[] args) {
    Utils.logMethodCall("Enter {}.{}({})", method.getDeclaringClass(), method, args);
    return (T) completedFuture(null);
  }
}
