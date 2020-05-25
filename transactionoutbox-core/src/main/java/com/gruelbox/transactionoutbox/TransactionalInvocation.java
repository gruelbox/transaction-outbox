package com.gruelbox.transactionoutbox;

import lombok.Value;

/**
 * Describes a method invocation along with the transaction scope in which it should be performed.
 */
@SuppressWarnings("WeakerAccess")
@Value
public class TransactionalInvocation<TX extends Transaction<?, ?>> {
  Class<?> clazz;
  String methodName;
  Class<?>[] parameters;
  Object[] args;
  TX transaction;
}
