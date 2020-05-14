package com.gruelbox.transactionoutbox;

import com.google.gson.annotations.SerializedName;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

/**
 * Represents the invocation of a specific method on a named class (where the name is provided by an
 * {@link Instantiator}), with the specified arguments.
 */
@SuppressWarnings("WeakerAccess")
@Value
@Slf4j
public class Invocation {

  /** The class name (as provided/expected by an {@link Instantiator}). */
  @SerializedName("c")
  String className;

  /** The method name. Combined with {@link #parameterTypes}, uniquely identifies the method. */
  @SerializedName("m")
  String methodName;

  /**
   * The method parameter types. Combined with {@link #methodName}, uniquely identifies the method.
   */
  @SerializedName("p")
  Class<?>[] parameterTypes;

  /** The arguments to call. Must match {@link #parameterTypes}. */
  @SerializedName("a")
  Object[] args;

  /** Thread-local context to recreate when running the task. */
  @SerializedName("x")
  Map<String, String> mdc;

  public Invocation(String className, String methodName, Class<?>[] parameterTypes, Object[] args) {
    this(className, methodName, parameterTypes, args, null);
  }

  public Invocation(String className, String methodName, Class<?>[] parameterTypes, Object[] args, Map<String, String> mdc) {
    this.className = className;
    this.methodName = methodName;
    this.parameterTypes = parameterTypes;
    this.args = args;
    this.mdc = mdc;
  }

  void invoke(Object instance) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Method method = instance.getClass().getDeclaredMethod(methodName, parameterTypes);
    method.setAccessible(true);
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
}
