package com.gruelbox.transactionoutbox.testing;

public interface ThrowingProcessor {
  void process() throws Exception;
}
