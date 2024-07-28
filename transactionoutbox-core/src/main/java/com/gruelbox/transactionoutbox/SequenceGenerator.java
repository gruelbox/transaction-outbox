package com.gruelbox.transactionoutbox;

/**
 * Generates sequences for a topic that is used in ordered tasks.
 * For most use cases, just use {@link DefaultSequenceGenerator}.
 */
public interface SequenceGenerator {
  /**
   * Returns the sequence number for a topic
   *
   * @param tx The current {@link Transaction}
   * @param topic The topic. Can be considered as a key in your storage
   * @return The sequence number for a topic
   * @throws Exception Any exception
   */
  long generate(Transaction tx, String topic) throws Exception;
}
