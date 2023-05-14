package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.Utils;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class InsertingInstantiator implements Instantiator {

  private final Function<Object, CompletableFuture<?>> inserter;

  @Override
  public String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  @SneakyThrows
  public Object getInstance(String name) {
    return FailingInstantiator.PROXY_FACTORY.createProxy(Class.forName(name), this::invoke);
  }

  @SuppressWarnings("unchecked")
  private <T> T invoke(Method method, Object[] args) {
    Utils.logMethodCall("Enter {}.{}({})", method.getDeclaringClass(), method, args);
    try {
      return (T) inserter.apply(args[2]);
    } catch (Exception e) {
      log.error("Error in task", e);
      throw e;
    }
  }
}
