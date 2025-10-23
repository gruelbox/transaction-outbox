package com.gruelbox.transactionoutbox;

public class TestingMode {

  public static void enable() {
    TransactionOutboxImpl.FORCE_SERIALIZE_AND_DESERIALIZE_BEFORE_USE.set(true);
  }

  public static void disable() {
    TransactionOutboxImpl.FORCE_SERIALIZE_AND_DESERIALIZE_BEFORE_USE.set(false);
  }
}
