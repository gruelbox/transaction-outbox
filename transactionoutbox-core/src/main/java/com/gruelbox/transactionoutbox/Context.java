package com.gruelbox.transactionoutbox;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use to annotate parameters on any method invoked via {@link TransactionOutbox#schedule(Class)}
 * which should receive the transaction context in which the task is eventually run. This is only
 * applicable to {@link TransactionManager} implementations which do not use thread-local context
 * and instead rely on it being passed down the stack explicitly. The parameter should be typed to
 * receive whatever the transaction manager treats as context.
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Context {}
