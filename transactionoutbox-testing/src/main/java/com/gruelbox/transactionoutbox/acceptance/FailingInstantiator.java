package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.ProxyFactory;
import com.gruelbox.transactionoutbox.Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

@Slf4j
public class FailingInstantiator implements Instantiator {

  static final ProxyFactory PROXY_FACTORY = new ProxyFactory();

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
    return PROXY_FACTORY.createProxy(
            Class.forName(name),
            this::invoke);
  }

  @SuppressWarnings("unchecked")
  private <T> T invoke(Method method, Object[] args) {
    if (verbose) Utils.logMethodCall("Enter {}.{}({})", method.getDeclaringClass(), method, args);
    if (attempts.incrementAndGet() <= failuresUntilSuccess) {
      if (verbose) Utils.logMethodCall("Failed {}.{}({})", method.getDeclaringClass(), method, args);
      if (CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
        return (T) failedFuture(new RuntimeException("Temporary failure"));
      } else {
        throw new RuntimeException("Temporary failure");
      }
    }
    if (verbose) Utils.logMethodCall("Exit {}.{}({})", method.getDeclaringClass(), method, args);
    return (T) completedFuture(null);
  }
}
