package com.gruelbox.transactionoutbox;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.joining;

import java.lang.StackWalker.StackFrame;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Utility methods used by transaction outbox. These are very firmly {@link NotApi}. Don't use them
 * in your code as they may be modified or removed without warning.
 */
@Slf4j
@NotApi
public class Utils {

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

  public static <T> T createLoggingProxy(ProxyFactory proxyFactory, Class<T> clazz) {
    return proxyFactory.createProxy(
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

  public static Optional<StackFrame> traceCaller() {
    StackWalker stackWalker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    return stackWalker.walk(stream1 -> stream1.skip(2).findFirst());
  }

  @SneakyThrows
  public static RuntimeException sneakyThrow(Throwable t) {
    if (t instanceof CompletionException) {
      throw t.getCause();
    } else {
      throw t;
    }
  }

  public static void sneakyThrowing(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      throw sneakyThrow(e);
    }
  }
}
