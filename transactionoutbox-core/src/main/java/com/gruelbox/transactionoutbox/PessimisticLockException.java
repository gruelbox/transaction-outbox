package com.gruelbox.transactionoutbox;

/** Thrown when we get a deadlock trying to access a record. */
@Beta
public class PessimisticLockException extends LockException {

  public PessimisticLockException(Throwable cause) {
    super(cause);
  }
}
