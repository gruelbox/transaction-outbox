package com.gruelbox.transactionoutbox;

/** A function... that throws. */
@SuppressWarnings("WeakerAccess")
public interface ThrowingFunction<T, U> {

  U apply(T t) throws Exception;
}
