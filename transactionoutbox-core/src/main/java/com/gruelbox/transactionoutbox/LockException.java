package com.gruelbox.transactionoutbox;

@Beta
public class LockException extends RuntimeException {
  LockException() {
    super();
  }

  LockException(Throwable cause) {
    super(cause);
  }
}
