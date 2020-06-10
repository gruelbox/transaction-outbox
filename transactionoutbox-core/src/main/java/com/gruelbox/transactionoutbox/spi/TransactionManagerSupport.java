package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionContextPlaceholder;
import com.gruelbox.transactionoutbox.TransactionalInvocation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/**
 * Various static methods intended to support creation of {@link BaseTransactionManager}
 * implementations.
 */
@Beta
public final class TransactionManagerSupport {

  private TransactionManagerSupport() {}

  /**
   * Obtains the active transaction by parsing the method arguments for a {@link BaseTransaction} or
   * a context object. All such arguments are removed from the invocation and replaced with nulls
   * before saving. They will be "rehydrated" later upon actual invocation using the
   * transaction/context at the time of invocation ({@link
   * #injectTransactionIntoInvocation(Invocation, Class, BaseTransaction)} can assist with this).
   *
   * @param <CX> The type expected for the transaction context.
   * @param method The method called.
   * @param args The method arguments.
   * @param contextType The type expected for the transaction context.
   * @param transactionFromContext A function for determining the transaction for a given context.
   * @return The transactional invocation, or throws {@link IllegalStateException}
   */
  public static <CX> TransactionalInvocation extractTransactionFromInvocation(
      Method method,
      Object[] args,
      Class<CX> contextType,
      Function<CX, ? extends BaseTransaction<?>> transactionFromContext) {
    return extractTransactionFromInvocationOptional(
            method, args, contextType, transactionFromContext)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Transaction context (either "
                        + contextType.getName()
                        + " or "
                        + BaseTransaction.class.getName()
                        + ") must be passed as a parameter to any scheduled method."));
  }

  /**
   * Obtains the active transaction by parsing the method arguments for a {@link BaseTransaction} or
   * a context object. All such arguments are removed from the invocation adn replaced with nulls
   * before saving. They will be "rehydrated" later upon actual invocation using the
   * transaction/context at the time of invocation.
   *
   * @param <CX> The type expected for the transaction context.
   * @param method The method called.
   * @param oldArgs The method arguments.
   * @param contextType The type expected for the transaction context.
   * @param transactionFromContext A function for determining the transaction for a given context.
   * @return The transactional invocation.
   */
  @SuppressWarnings("unchecked")
  public static <CX> Optional<TransactionalInvocation> extractTransactionFromInvocationOptional(
      Method method,
      Object[] oldArgs,
      Class<CX> contextType,
      Function<CX, ? extends BaseTransaction<?>> transactionFromContext) {
    Object[] newArgs = Arrays.copyOf(oldArgs, oldArgs.length);
    var params = Arrays.copyOf(method.getParameterTypes(), method.getParameterCount());
    BaseTransaction<?> transaction = null;
    for (int i = 0; i < newArgs.length; i++) {
      Object candidate = newArgs[i];
      if (candidate instanceof BaseTransaction) {
        transaction = (BaseTransaction<?>) candidate;
        newArgs[i] = null;
      } else if (contextType.isInstance(candidate)) {
        if (transaction == null) {
          transaction = transactionFromContext.apply((CX) candidate);
          if (transaction == null) {
            throw new IllegalArgumentException(
                candidate.getClass().getName()
                    + " context passed to "
                    + method
                    + " does not relate to a known transaction. This either indicates that the context object was not "
                    + "created by normal means or the transaction manager is incorrectly configured.");
          }
        }
        newArgs[i] = null;
        params[i] = TransactionContextPlaceholder.class;
      }
    }
    return Optional.ofNullable(transaction)
        .map(
            tx ->
                new TransactionalInvocation(
                    method.getDeclaringClass(), method.getName(), params, newArgs, tx));
  }

  /**
   * Modifies an {@link Invocation} at runtime to rehyrate it with the transaction context in which
   * the record was locked.
   *
   * @param invocation The invocation.
   * @param contextType The type expected for the transaction context.
   * @param transaction The transaction to use.
   * @return The modified invocation.
   */
  public static Invocation injectTransactionIntoInvocation(
      Invocation invocation, Class<?> contextType, BaseTransaction<?> transaction) {
    Object[] args = Arrays.copyOf(invocation.getArgs(), invocation.getArgs().length);
    Class<?>[] params =
        Arrays.copyOf(invocation.getParameterTypes(), invocation.getParameterTypes().length);
    for (int i = 0; i < invocation.getParameterTypes().length; i++) {
      Class<?> parameterType = invocation.getParameterTypes()[i];
      if (BaseTransaction.class.isAssignableFrom(parameterType)) {
        if (args[i] != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s.%s[%d] contains unexpected serialized Transaction",
                  invocation.getClassName(), invocation.getMethodName(), i));
        }
        args[i] = transaction;
      } else if (parameterType.equals(TransactionContextPlaceholder.class)) {
        if (args[i] != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s.%s[%d] contains unexpected serialized Transaction context",
                  invocation.getClassName(), invocation.getMethodName(), i));
        }
        args[i] = transaction.context();
        params[i] = contextType;
      }
    }
    return new Invocation(
        invocation.getClassName(), invocation.getMethodName(), params, args, invocation.getMdc());
  }

  public static TransactionalInvocation toTransactionalInvocation(
      Method method, Object[] args, BaseTransaction<?> transaction) {
    return new TransactionalInvocation(
        method.getDeclaringClass(),
        method.getName(),
        method.getParameterTypes(),
        args,
        transaction);
  }
}
