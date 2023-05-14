package com.gruelbox.transactionoutbox.acceptance;

import static com.gruelbox.transactionoutbox.acceptance.LoggingInstantiator.PROXY_FACTORY;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.Utils;
import java.lang.reflect.Method;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RandomFailingInstantiator implements Instantiator {

  private static final PrimitiveIterator.OfInt randoms = new Random().ints(0, 19).iterator();

  private final boolean verbose;

  public RandomFailingInstantiator() {
    this.verbose = true;
  }

  public RandomFailingInstantiator(boolean verbose) {
    this.verbose = verbose;
  }

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
    if (verbose) Utils.logMethodCall("Enter {}.{}({})", method.getDeclaringClass(), method, args);
    if (randoms.next() == 5) {
      if (verbose)
        Utils.logMethodCall("Failed {}.{}({})", method.getDeclaringClass(), method, args);
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
