package com.gruelbox.transactionoutbox;

/** Thrown if an active transaction is required by a method and no transaction is active. */
@SuppressWarnings("WeakerAccess")
public final class NoTransactionActiveException extends RuntimeException {

  public NoTransactionActiveException() {
    super();
  }

  public NoTransactionActiveException(Throwable cause) {
    super(cause);
  }
}
