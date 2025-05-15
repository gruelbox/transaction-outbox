package com.gruelbox.transactionoutbox;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/** Represents an invocation those deserialization failed. */
public class FailedDeserializingInvocation extends Invocation {

  /**
   * @return Indicates an Exception during De-Serialization, that is not persisted.
   */
  private final transient IOException exceptionDuringDeserialization;

  public FailedDeserializingInvocation(IOException exceptionDuringDeserialization) {
    super("", "", new Class<?>[] {}, new Object[] {});
    this.exceptionDuringDeserialization = exceptionDuringDeserialization;
  }

  @Override
  void invoke(Object instance, TransactionOutboxListener listener)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    throw new UncheckedException(exceptionDuringDeserialization);
  }
}
