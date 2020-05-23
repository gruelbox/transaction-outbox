package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import lombok.Value;

/**
 * Describes a method invocation along with the transaction scope in which it should be performed.
 */
@Value
public class TransactionalInvocation {
  Class<?> clazz;
  String methodName;
  Class<?>[] parameters;
  Object[] args;
  BaseTransaction<?> transaction;
}
