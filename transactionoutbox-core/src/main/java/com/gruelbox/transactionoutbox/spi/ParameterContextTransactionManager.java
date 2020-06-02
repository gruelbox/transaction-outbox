package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * A transaction manager "mixin" which adds the behaviour to make no assumption of a "current"
 * {@link Transaction}. This means that {@link TransactionOutbox#schedule(Class)} needs to be given
 * the transaction to use as part of any invoked method's arguments. In turn, that method will need
 * the transaction at the time it is invoked.
 *
 * <p>Call patterns permitted:
 *
 * <pre>
 * // Using TransactionManager
 * transactionManager.inTransaction(tx -&gt;
 *   outbox.schedule(MyClass.class).myMethod("foo", tx));
 *
 * // Using some third party transaction manager
 * wibbleTransactionManager.doInATransaction(context -&gt;
 *   outbox.schedule(MyClass.class).myMethod("foo", context));
 * </pre>
 *
 * @param <CN> The type which the associated {@link Persistor} implementation will use to interact
 *     with the data store.
 * @param <CX> The type that the client code uses to interact with the transaction.
 * @param <TX> The transaction type.
 */
@Beta
public interface ParameterContextTransactionManager<CN, CX, TX extends Transaction<CN, CX>>
    extends TransactionManager<CN, CX, TX> {

  /**
   * Given an implementation-specific transaction context, return the active {@link Transaction}.
   *
   * @param context The implementation-specific context, of the same type returned by {@link
   *     #contextType()}.
   * @return The transaction, or null if the context is not known.
   */
  TX transactionFromContext(CX context);

  /** @return The type expected by {@link #transactionFromContext(CX)}. */
  Class<CX> contextType();

  /**
   * Obtains the active transaction by parsing the method arguments for a {@link Transaction} or a
   * context (any object of type {@link #contextType()}). All such arguments are removed from the
   * invocation adn replaced with nulls before saving. They will be "rehydrated" later upon actual
   * invocation using the transaction/context at the time of invocation.
   *
   * @param method The method called.
   * @param args The method arguments.
   * @return The transactional invocation.
   */
  @SuppressWarnings("unchecked")
  @Override
  default TransactionalInvocation<TX> extractTransaction(Method method, Object[] args) {
    args = Arrays.copyOf(args, args.length);
    var params = Arrays.copyOf(method.getParameterTypes(), method.getParameterCount());
    TX transaction = null;
    for (int i = 0; i < args.length; i++) {
      Object candidate = args[i];
      if (candidate instanceof Transaction) {
        transaction = (TX) candidate;
        args[i] = null;
      } else if (contextType().isInstance(candidate)) {
        if (transaction == null) {
          transaction = transactionFromContext((CX) candidate);
          if (transaction == null) {
            throw new IllegalArgumentException(
                candidate.getClass().getName()
                    + " context passed to "
                    + method
                    + " does not relate to a known transaction. This either indicates that the context object was not "
                    + "created by normal means or the transaction manager is incorrectly configured.");
          }
        }
        args[i] = null;
        params[i] = TransactionContextPlaceholder.class;
      }
    }
    if (transaction == null) {
      throw new IllegalArgumentException(
          getClass().getName()
              + " requires transaction context (either "
              + contextType().getName()
              + " or "
              + Transaction.class.getName()
              + ") to be passed as a parameter to any scheduled method.");
    }
    return new TransactionalInvocation<>(
        method.getDeclaringClass(), method.getName(), params, args, transaction);
  }

  /**
   * Modifies an {@link Invocation} at runtime to rehyrate it with the transaction context in which
   * the record was locked.
   *
   * @param invocation The invocation.
   * @param transaction The transaction to use.
   * @return The modified invocation.
   */
  @Override
  default Invocation injectTransaction(Invocation invocation, TX transaction) {
    Object[] args = Arrays.copyOf(invocation.getArgs(), invocation.getArgs().length);
    Class<?>[] params =
        Arrays.copyOf(invocation.getParameterTypes(), invocation.getParameterTypes().length);
    for (int i = 0; i < invocation.getParameterTypes().length; i++) {
      Class<?> parameterType = invocation.getParameterTypes()[i];
      if (Transaction.class.isAssignableFrom(parameterType)) {
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
        params[i] = contextType();
      }
    }
    return new Invocation(
        invocation.getClassName(), invocation.getMethodName(), params, args, invocation.getMdc());
  }
}
