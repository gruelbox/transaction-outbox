package com.gruelbox.transactionoutbox;

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;

@Slf4j
class Utils {

  private static Class<?> cMethodInterceptor;
  private static Class<?> cCallback;
  private static MethodHandle enhancer_create;
  private static MethodHandle enhancer_constructor;
  private static MethodHandle enhancer_setSuperClass;
  private static MethodHandle enhancer_setCallbackTypes;
  private static MethodHandle enhancer_setInterceptDuringConstruction;
  private static MethodHandle enhancer_createClass;
  private static MethodHandle factory_setCallbacks;
  private static MethodHandle objenesis_getInstantiatorOf;
  private static MethodHandle objectInstantiator_newInstance;
  private static Object objenesis;

  static {
    Lookup lookup = MethodHandles.lookup();
    try {
      cCallback = Class.forName("net.sf.cglib.proxy.Callback");
      Class<?> cCallbackArray = Class.forName("[Lnet.sf.cglib.proxy.Callback;");
      cMethodInterceptor = Class.forName("net.sf.cglib.proxy.MethodInterceptor");
      Class<?> cEnhancer = Class.forName("net.sf.cglib.proxy.Enhancer");
      Class<?> cFactory = Class.forName("net.sf.cglib.proxy.Factory");
      enhancer_create =
          lookup.findStatic(cEnhancer, "create", methodType(Object.class, Class.class, cCallback));
      enhancer_constructor = lookup.findConstructor(cEnhancer, methodType(void.class));
      enhancer_setSuperClass =
          lookup.findVirtual(cEnhancer, "setSuperclass", methodType(void.class, Class.class));
      enhancer_setCallbackTypes =
          lookup.findVirtual(cEnhancer, "setCallbackTypes", methodType(void.class, Class[].class));
      enhancer_setInterceptDuringConstruction =
          lookup.findVirtual(
              cEnhancer, "setInterceptDuringConstruction", methodType(void.class, boolean.class));
      enhancer_createClass = lookup.findVirtual(cEnhancer, "createClass", methodType(Class.class));
      factory_setCallbacks =
          lookup.findVirtual(cFactory, "setCallbacks", methodType(void.class, cCallbackArray));
      log.info("CGLIB found and initialised");
    } catch (Throwable e) {
      log.info(
          "CGLIB is not available. Proxying of concrete classes will be disabled ("
              + e.getMessage()
              + ")");
    }
    try {
      Class<?> cObjenesis = Class.forName("org.objenesis.Objenesis");
      Class<?> cObjenesisStd = Class.forName("org.objenesis.ObjenesisStd");
      Class<?> cObjectInstantiator = Class.forName("org.objenesis.instantiator.ObjectInstantiator");
      objenesis = lookup.findConstructor(cObjenesisStd, methodType(void.class)).invoke();
      objenesis_getInstantiatorOf =
          lookup.findVirtual(
              cObjenesis, "getInstantiatorOf", methodType(cObjectInstantiator, Class.class));
      objectInstantiator_newInstance =
          lookup.findVirtual(cObjectInstantiator, "newInstance", methodType(Object.class));
      log.info("Objenesis found and initialised");
    } catch (Throwable e) {
      log.info(
          "Objenesis is not available. Proxying of concrete classes with non-default "
              + "constructors will be disabled ("
              + e.getMessage()
              + ")");
    }
  }

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

  @SuppressWarnings({"unchecked", "cast"})
  static <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], T> processor) {
    if (clazz.isInterface()) {
      // Fastest - we can just proxy an interface directly
      return (T)
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              (proxy, method, args) -> processor.apply(method, args));
    } else if (hasDefaultConstructor(clazz)) {
      // CGLIB on its own can create an instance
      return createCglibProxy(clazz, (proxy, method, args) -> processor.apply(method, args));
    } else {
      // Slowest - we need to use Objenesis and CGLIB together
      return createObjenesisProxy(clazz, (proxy, method, args) -> processor.apply(method, args));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T createCglibProxy(Class<T> clazz, InvocationHandler handler) {
    if (enhancer_create == null) {
      throw new UnsupportedOperationException(
          "To proxy concrete classes, CGLIB 3.x.x is required "
              + "on the classpath. It is available on Maven Central.");
    }
    try {
      return (T) enhancer_create.invoke(clazz, toMethodInterceptor(handler));
    } catch (Throwable e) {
      Utils.uncheckAndThrow(e);
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T createObjenesisProxy(Class<T> clazz, InvocationHandler handler) {
    if (enhancer_createClass == null) {
      throw new UnsupportedOperationException(
          "To proxy concrete classes which lack a default "
              + "constructor, both CGLIB 3.x.x and Objenesis 3.x are required on the classpath. Both "
              + "are available on Maven Central.");
    }
    try {
      Object methodInterceptor = toMethodInterceptor(handler);
      Object enhancer = enhancer_constructor.invoke();
      enhancer_setSuperClass.invoke(enhancer, clazz);
      enhancer_setCallbackTypes.invoke(enhancer, new Class<?>[] {cMethodInterceptor});
      enhancer_setInterceptDuringConstruction.invoke(enhancer, true);
      Class<T> proxyClass = (Class<T>) enhancer_createClass.invoke(enhancer);
      // TODO could cache the ObjectInstantiators - see ObjenesisSupport in spring-aop
      Object objectInstantiator = objenesis_getInstantiatorOf.invoke(objenesis, proxyClass);
      T proxy = (T) objectInstantiator_newInstance.invoke(objectInstantiator);
      Object callbacks = Array.newInstance(cCallback, 1);
      Array.set(callbacks, 0, methodInterceptor);
      factory_setCallbacks.invoke(proxy, callbacks);
      enhancer_setInterceptDuringConstruction.invoke(enhancer, false);
      return proxy;
    } catch (Throwable e) {
      Utils.uncheckAndThrow(e);
      throw new RuntimeException(e);
    }
  }

  private static Object toMethodInterceptor(InvocationHandler handler) {
    return Proxy.newProxyInstance(
        cMethodInterceptor.getClassLoader(),
        new Class[] {cMethodInterceptor},
        (proxy, method, args) -> handler.invoke(args[0], (Method) args[1], (Object[]) args[2]));
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
