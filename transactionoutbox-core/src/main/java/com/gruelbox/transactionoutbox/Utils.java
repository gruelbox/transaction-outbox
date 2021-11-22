package com.gruelbox.transactionoutbox;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;
import org.slf4j.Logger;
import org.slf4j.event.Level;

@Slf4j
class Utils {

  private static final Objenesis objenesis = new ObjenesisStd();

  @SuppressWarnings({"SameParameterValue", "WeakerAccess", "UnusedReturnValue"})
  static boolean safelyRun(String gerund, ThrowingRunnable runnable) {
    try {
      runnable.run();
      return true;
    } catch (Exception e) {
      log.error("Error when {}", gerund, e);
      return false;
    }
  }

  @SuppressWarnings("unused")
  static void safelyClose(AutoCloseable... closeables) {
    safelyClose(Arrays.asList(closeables));
  }

  static void safelyClose(Iterable<? extends AutoCloseable> closeables) {
    closeables.forEach(
        d -> {
          if (d == null) return;
          safelyRun("closing resource", d::close);
        });
  }

  static void uncheck(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      uncheckAndThrow(e);
    }
  }

  static <T> T uncheckedly(Callable<T> runnable) {
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

  public static class ProxyDelegator<T> {

    private final BiFunction<Method, Object[], T> processor;

    public ProxyDelegator(BiFunction<Method, Object[], T> processor) {
      this.processor = processor;
    }

    @RuntimeType
    public Object intercept(@Origin Method m, @AllArguments Object... args) {
      return processor.apply(m, args);
    }
  }

  @SuppressWarnings({"unchecked", "cast"})
  static <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], T> processor) {
    if (clazz.isInterface()) {
      // Fastest - we can just proxy an interface directly
      return (T)
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              (proxy, method, args) -> processor.apply(method, args));
    } else {
      Class<? extends T> proxy =
          new ByteBuddy()
              .subclass(clazz)
              .method(ElementMatchers.isDeclaredBy(clazz))
              .intercept(MethodDelegation.to(new ProxyDelegator<>(processor)))
              .make()
              .load(clazz.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
              .getLoaded();
      if (hasDefaultConstructor(clazz)) {
        return uncheckedly(() -> proxy.getDeclaredConstructor().newInstance());
      } else {
        ObjectInstantiator<? extends T> instantiator = objenesis.getInstantiatorOf(proxy);
        return instantiator.newInstance();
      }
    }
  }

  static <T> T createLoggingProxy(Class<T> clazz) {
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
