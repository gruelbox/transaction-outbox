package com.gruelbox.transactionoutbox;

import com.google.gson.annotations.SerializedName;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * Represents the invocation of a specific method on a named class (where the name is provided by an
 * {@link Instantiator}), with the specified arguments.
 *
 * <p>Optimized for safe serialization via GSON.
 */
@SuppressWarnings("WeakerAccess")
@Value
@Slf4j
public class Invocation {

  /** @return The class name (as provided/expected by an {@link Instantiator}). */
  @SuppressWarnings("JavaDoc")
  @SerializedName("c")
  String className;

  /**
   * @return The method name. Combined with {@link #parameterTypes}, uniquely identifies the method.
   */
  @SuppressWarnings("JavaDoc")
  @SerializedName("m")
  String methodName;

  /**
   * @return The method parameter types. Combined with {@link #methodName}, uniquely identifies the
   *     method.
   */
  @SuppressWarnings("JavaDoc")
  @SerializedName("p")
  Class<?>[] parameterTypes;

  /** @return The arguments to call. Must match {@link #parameterTypes}. */
  @SuppressWarnings("JavaDoc")
  @SerializedName("a")
  Object[] args;

  /** @return Thread-local context to recreate when running the task. */
  @SuppressWarnings("JavaDoc")
  @SerializedName("x")
  Map<String, String> mdc;

  /**
   * @param className The class name (as provided/expected by an {@link Instantiator}).
   * @param methodName The method name. Combined with {@link #parameterTypes}, uniquely identifies
   *     the method.
   * @param parameterTypes The method parameter types. Combined with {@link #methodName}, uniquely
   *     identifies the method.
   * @param args The arguments to call. Must match {@link #parameterTypes}.
   */
  public Invocation(String className, String methodName, Class<?>[] parameterTypes, Object[] args) {
    this(className, methodName, parameterTypes, args, null);
  }

  /**
   * @param className The class name (as provided/expected by an {@link Instantiator}).
   * @param methodName The method name. Combined with {@link #parameterTypes}, uniquely identifies
   *     the method.
   * @param parameterTypes The method parameter types. Combined with {@link #methodName}, uniquely
   *     identifies the method.
   * @param args The arguments to call. Must match {@link #parameterTypes}.
   * @param mdc Thread-local context to recreate when running the task.
   */
  public Invocation(
      String className,
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args,
      Map<String, String> mdc) {
    this.className = className;
    this.methodName = methodName;
    this.parameterTypes = parameterTypes;
    this.args = args;
    this.mdc = mdc;
  }

  void invoke(Object instance, Transaction transaction)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Method method = instance.getClass().getDeclaredMethod(methodName, parameterTypes);
    method.setAccessible(true);
    Object[] args = injectArguments(method, transaction);
    if (log.isDebugEnabled()) {
      log.debug("Invoking method {} with args {}", method, Arrays.toString(args));
    }
    if (mdc != null && MDC.getMDCAdapter() != null) {
      var oldMdc = MDC.getCopyOfContextMap();
      MDC.setContextMap(mdc);
      try {
        method.invoke(instance, args);
      } finally {
        if (oldMdc == null) {
          MDC.clear();
        } else {
          MDC.setContextMap(oldMdc);
        }
      }
    } else {
      method.invoke(instance, args);
    }
  }

  private Object[] injectArguments(Method method, Transaction transaction) {
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> paramType = parameterTypes[i];
      if (Transaction.class.isAssignableFrom(parameterTypes[i])) {
        if (args[i] != null) {
          throw new IllegalArgumentException("Unexpected serialized transaction in invocation");
        }
        Object[] args = Arrays.copyOf(this.args, this.args.length);
        args[i] = transaction;
        return args;
      }
      if (Arrays.stream(parameterAnnotations[i])
          .anyMatch(annotation -> annotation instanceof Context)) {
        if (args[i] != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s.%s[%d] is annotated with Context but invocation contains serialized content",
                  method.getDeclaringClass().getName(), method.getName(), i));
        }
        if (transaction.context() == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s.%s[%d] is annotated with Context but no context is provided by transaction manager",
                  method.getDeclaringClass().getName(), method.getName(), i));
        }
        Object[] args = Arrays.copyOf(this.args, this.args.length);
        args[i] = transaction.context();
        return args;
      }
    }
    return args;
  }
}
