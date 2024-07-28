package com.gruelbox.transactionoutbox;

public interface SequenceGenerator {
  long generate(Transaction tx, String topic) throws Exception;
}
