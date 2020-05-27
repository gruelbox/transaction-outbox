package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.event.Level;

@Slf4j
public class Utils {

  private static final Objenesis objenesis = new ObjenesisStd();

  @SuppressWarnings({"SameParameterValue", "WeakerAccess", "UnusedReturnValue"})
  public static boolean safelyRun(String gerund, ThrowingRunnable runnable) {
    try {
      runnable.run();
      return true;
    } catch (Exception e) {
      log.error("Error when {}", gerund, e);
      return false;
    }
  }

  @SuppressWarnings("unused")
  public static void safelyClose(AutoCloseable... closeables) {
    safelyClose(Arrays.asList(closeables));
  }

  public static void safelyClose(Iterable<? extends AutoCloseable> closeables) {
    closeables.forEach(
        d -> {
          if (d == null) return;
          safelyRun("closing resource", d::close);
        });
  }

  public static void uncheck(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      uncheckAndThrow(e);
    }
  }

  public static <T> T uncheckedly(Callable<T> runnable) {
    try {
      return runnable.call();
    } catch (Exception e) {
      return uncheckAndThrow(e);
    }
  }

  static <T> T uncheckAndThrow(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    throw new UncheckedException(e);
  }

  public static <T> CompletableFuture<T> toRunningFuture(Publisher<T> publisher) {
    CompletableFuture<T> future = new CompletableFuture<>();
    publisher.subscribe(
        new Subscriber<>() {

          private volatile T result;

          @Override
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(T t) {
            result = t;
          }

          @Override
          public void onError(Throwable t) {
            future.completeExceptionally(t);
          }

          @Override
          public void onComplete() {
            future.complete(result);
          }
        });
    return future;
  }

  public static <T> CompletableFuture<T> toBlockingFuture(Callable<T> callable) {
    try {
      return completedFuture(callable.call());
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  public static CompletableFuture<Void> toBlockingFuture(ThrowingRunnable runnable) {
    try {
      runnable.run();
      return completedFuture(null);
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  public static <T> T blockingRun(Future<T> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw (RuntimeException) Utils.uncheckAndThrow(e.getCause());
    }
  }

  @SuppressWarnings({"unchecked", "cast"})
  static <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], Object> processor) {
    if (clazz.isInterface()) {
      // Fastest - we can just proxy an interface directly
      return (T)
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              (proxy, method, args) -> processor.apply(method, args));
    } else if (hasDefaultConstructor(clazz)) {
      // CGLIB on its own can create an instance
      return (T)
          Enhancer.create(
              clazz,
              (MethodInterceptor)
                  (o, method, objects, methodProxy) -> processor.apply(method, objects));
    } else {
      // Slowest - we need to use Objenesis and CGLIB together
      MethodInterceptor methodInterceptor =
          (o, method, objects, methodProxy) -> processor.apply(method, objects);
      Enhancer enhancer = new Enhancer();
      enhancer.setSuperclass(clazz);
      enhancer.setCallbackTypes(new Class<?>[] {MethodInterceptor.class});
      enhancer.setInterceptDuringConstruction(true);
      Class<T> proxyClass = enhancer.createClass();
      // TODO could cache the ObjectInstantiators - see ObjenesisSupport in spring-aop
      ObjectInstantiator<T> oi = objenesis.getInstantiatorOf(proxyClass);
      T proxy = oi.newInstance();
      ((net.sf.cglib.proxy.Factory) proxy).setCallbacks(new Callback[] {methodInterceptor});
      enhancer.setInterceptDuringConstruction(false);
      return proxy;
    }
  }

  public static <T> T createLoggingProxy(Class<T> clazz) {
    return createProxy(
        clazz,
        (method, args) -> {
          log.info(
              "Called mock " + clazz.getSimpleName() + ".{}({})",
              method.getName(),
              args == null
                  ? ""
                  : Arrays.stream(args)
                      .map(it -> it == null ? "null" : it.toString())
                      .collect(Collectors.joining(", ")));
          return null;
        });
  }

  static <T> T firstNonNull(T one, Supplier<T> two) {
    if (one == null) return two.get();
    return one;
  }

  static void logAtLevel(Logger logger, Level level, String message, Object... args) {
    switch (level) {
      case ERROR:
        logger.error(message, args);
        break;
      case WARN:
        logger.warn(message, args);
        break;
      case INFO:
        logger.info(message, args);
        break;
      case DEBUG:
        logger.debug(message, args);
        break;
      case TRACE:
        logger.trace(message, args);
        break;
      default:
        logger.warn(message, args);
        break;
    }
  }

  private static boolean hasDefaultConstructor(Class<?> clazz) {
    try {
      clazz.getConstructor();
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
