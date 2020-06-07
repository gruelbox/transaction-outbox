package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.joining;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

@Slf4j
public class Utils {

  private static final Objenesis objenesis = new ObjenesisStd();

  private Utils() {}

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

  public static <T> T uncheckAndThrow(Throwable e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    throw new UncheckedException(e);
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

  @SneakyThrows
  public static <T> T join(CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @SuppressWarnings({"unchecked", "cast"})
  public static <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], Object> processor) {
    BiFunction<Method, Object[], Object> wrapped = (method, args) -> {
      switch (method.getName()) {
        case "toString": return "Proxy[" + clazz.getName() + "]";
        case "hashCode": return processor.hashCode();
        case "equals": return false;
        default: return processor.apply(method, args);
      }
    };
    if (clazz.isInterface()) {
      // Fastest - we can just proxy an interface directly
      return (T)
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              (proxy, method, args) -> wrapped.apply(method, args));
    } else if (hasDefaultConstructor(clazz)) {
      // CGLIB on its own can create an instance
      return (T)
          Enhancer.create(
              clazz,
              (MethodInterceptor)
                  (o, method, objects, methodProxy) -> wrapped.apply(method, objects));
    } else {
      // Slowest - we need to use Objenesis and CGLIB together
      MethodInterceptor methodInterceptor =
          (o, method, objects, methodProxy) -> wrapped.apply(method, objects);
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
          logMethodCall("Called mock {}.{}({})", clazz, method, args);
          return null;
        });
  }

  public static void logMethodCall(String format, Class<?> clazz, Method method, Object[] args) {
    LoggerFactory.getLogger(clazz)
        .info(
            format,
            clazz.getName(),
            method.getName(),
            args == null
                ? ""
                : Arrays.stream(args)
                    .map(it -> it == null ? "null" : it.toString())
                    .collect(joining(", ")));
  }

  public static <T> T firstNonNull(T one, Supplier<T> two) {
    if (one == null) return two.get();
    return one;
  }

  public static boolean logAtLevel(Logger logger, Level level, String message, Object... args) {
    switch (level) {
      case ERROR:
        if (logger.isErrorEnabled()) {
          logger.error(message, args);
          return true;
        } else {
          return false;
        }
      case WARN:
        logger.warn(message, args);
        if (logger.isWarnEnabled()) {
          logger.warn(message, args);
          return true;
        } else {
          return false;
        }
      case INFO:
        if (logger.isInfoEnabled()) {
          logger.info(message, args);
          return true;
        } else {
          return false;
        }
      case DEBUG:
        if (logger.isDebugEnabled()) {
          logger.debug(message, args);
          return true;
        } else {
          return false;
        }
      case TRACE:
        if (logger.isTraceEnabled()) {
          logger.trace(message, args);
          return true;
        } else {
          return false;
        }
      default:
        return logAtLevel(logger, Level.WARN, message, args);
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
