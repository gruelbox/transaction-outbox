package com.gruelbox.transactionoutbox;

import java.lang.reflect.Method;
import lombok.Value;

/**
 * Describes a method invocation along with the transaction scope in which it should be performed.
 */
@Value
public class TransactionalInvocation {
  Method method;
  Object[] args;
  Transaction transaction;
}
