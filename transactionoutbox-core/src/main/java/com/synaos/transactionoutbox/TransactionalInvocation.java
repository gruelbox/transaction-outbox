package com.synaos.transactionoutbox;

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
    Transaction transaction;
}
