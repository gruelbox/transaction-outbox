package com.gruelbox.transactionoutbox;

import com.google.gson.annotations.SerializedName;
import lombok.Value;

/**
 * Represents the invocation of a specific method on a named class (where the name is provided by an
 * {@link Instantiator}), with the specified arguments.
 */
@SuppressWarnings("WeakerAccess")
@Value
public class Invocation {

  /** The class name (as provided/expected by an {@link Instantiator}). */
  @SerializedName("c")
  private final String className;

  /** The method name. Combined with {@link #parameterTypes}, uniquely identifies the method. */
  @SerializedName("m")
  private final String methodName;

  /**
   * The method parameter types. Combined with {@link #methodName}, uniquely identifies the method.
   */
  @SerializedName("p")
  private final Class<?>[] parameterTypes;

  /** The arguments to call. Must match {@link #parameterTypes}. */
  @SerializedName("a")
  private final Object[] args;
}
