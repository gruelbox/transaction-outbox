package com.gruelbox.transactionoutbox;

public interface PersistorListener {

  PersistorListener EMPTY = new PersistorListener() {};

  default void beforeFirstSequenceAssigned(TransactionOutboxEntry entry) {
    // No-op
  }
}
