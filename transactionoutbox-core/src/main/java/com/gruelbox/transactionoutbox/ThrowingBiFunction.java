package com.gruelbox.transactionoutbox;

/** A function... that throws. */
@FunctionalInterface
public interface ThrowingBiFunction<T, U, V> {

  V apply(T t, U u) throws Exception;
}
