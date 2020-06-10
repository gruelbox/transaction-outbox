package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Describes a method invocation along with the transaction scope in which it should be performed.
 */
@EqualsAndHashCode
@ToString
public final class TransactionalInvocation {
  private final Class<?> clazz;
  private final String methodName;
  private final Class<?>[] parameters;
  private final Object[] args;
  private final BaseTransaction<?> transaction;

  /**
   * @param clazz The class to be invoked.
   * @param methodName The method name to call.
   * @param parameters The method parameter types. Combined with the method name, forms the
   *     signature of the method.
   * @param args The arguments with which to call the method.
   * @param transaction The transaction in which the method should be invoked.
   */
  public TransactionalInvocation(
      Class<?> clazz,
      String methodName,
      Class<?>[] parameters,
      Object[] args,
      BaseTransaction<?> transaction) {
    this.clazz = clazz;
    this.methodName = methodName;
    this.parameters = parameters;
    this.args = args;
    this.transaction = transaction;
  }

  /** @return The class to be invoked. */
  public Class<?> getClazz() {
    return this.clazz;
  }

  /** @return The method name to call. */
  public String getMethodName() {
    return this.methodName;
  }

  /**
   * @return The method parameter types. Combined with the method name, forms the signature of the
   *     method.
   */
  public Class<?>[] getParameters() {
    return this.parameters;
  }

  /** @return The arguments with which to call the method. */
  public Object[] getArgs() {
    return this.args;
  }

  /** @return The transaction in which the method should be invoked. */
  public BaseTransaction<?> getTransaction() {
    return this.transaction;
  }
}
