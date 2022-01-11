package com.gruelbox.transactionoutbox.acceptance;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailingInstantiator implements Instantiator {

  private final AtomicInteger attempts = new AtomicInteger();
  private final boolean verbose;
  private final int failuresUntilSuccess;

  public FailingInstantiator(int failuresUntilSuccess) {
    this.verbose = false;
    this.failuresUntilSuccess = failuresUntilSuccess;
  }

  public FailingInstantiator(int failuresUntilSuccess, boolean verbose) {
    this.verbose = verbose;
    this.failuresUntilSuccess = failuresUntilSuccess;
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
          if (verbose) Utils.logMethodCall("Enter {}.{}({})", clazz, method, args);
          if (attempts.incrementAndGet() <= failuresUntilSuccess) {
            if (verbose) Utils.logMethodCall("Failed {}.{}({})", clazz, method, args);
            if (CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
              return failedFuture(new RuntimeException("Temporary failure"));
            } else {
              throw new RuntimeException("Temporary failure");
            }
          }
          if (verbose) Utils.logMethodCall("Exit {}.{}({})", clazz, method, args);
          return completedFuture(null);
        });
  }
}