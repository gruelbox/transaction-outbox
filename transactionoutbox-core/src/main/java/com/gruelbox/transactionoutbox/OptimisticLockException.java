package com.gruelbox.transactionoutbox;

/** Thrown when we attempt to update a record which has been modified by another thread. */
public class OptimisticLockException extends LockException {

  public OptimisticLockException() {
    super();
  }
}
