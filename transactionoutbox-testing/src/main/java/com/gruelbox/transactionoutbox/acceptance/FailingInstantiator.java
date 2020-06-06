package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.Utils;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailingInstantiator implements Instantiator {

  private final AtomicInteger attempts;

  FailingInstantiator(AtomicInteger attempts) {
    this.attempts = attempts;
  }

  @Override
  public String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  @SneakyThrows
  public Object getInstance(String name) {
    Class<?> clazz = Class.forName(name);
    return Utils.createProxy(
        clazz,
        (method, args) -> {
          Utils.logMethodCall("Enter {}.{}({})", clazz, method, args);
          if (attempts.incrementAndGet() < 3) {
            return failedFuture(new RuntimeException("Temporary failure"));
          }
          Utils.logMethodCall("Exit {}.{}({})", clazz, method, args);
          return completedFuture(null);
        });
  }
}
