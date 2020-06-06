package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingInstantiator implements Instantiator {

  @Override
  public String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  @SneakyThrows
  public Object getInstance(String name) {
    return Utils.createProxy(
        Class.forName(name),
        (method, args) -> {
          Utils.logMethodCall("Enter {}.{}({})", method.getDeclaringClass(), method, args);
          return completedFuture(null);
        });
  }
}
