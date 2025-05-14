package com.gruelbox.transactionoutbox;


/** Thrown when de-serialization of an invocation fails. */
class DeserializationFailedException extends RuntimeException {
  DeserializationFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
