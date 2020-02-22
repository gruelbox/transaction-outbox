package com.gruelbox.transactionoutbox;

/** A wrapped {@link Exception} where unchecked exceptions are caught and propagated as runtime. */
@SuppressWarnings("WeakerAccess")
public class UncheckedException extends RuntimeException {

  public UncheckedException(Throwable cause) {
    super(cause);
  }
}
