package com.gruelbox.transactionoutbox;

import lombok.Value;

/**
 * Describes a method invocation along with the transaction scope in which it should be performed.
 */
@SuppressWarnings("WeakerAccess")
@Value
public class TransactionalInvocation<T> {
  Class<?> clazz;
  String methodName;
  Class<?>[] parameters;
  Object[] args;
  T transaction;
}
