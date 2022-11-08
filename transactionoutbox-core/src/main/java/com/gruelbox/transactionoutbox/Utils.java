package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
class Utils {

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

    static <T> T createLoggingProxy(ProxyFactory proxyFactory, Class<T> clazz) {
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
}
