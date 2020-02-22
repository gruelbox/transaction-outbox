package com.gruelbox.transactionoutbox;

/** A runnable... that throws. */
public interface ThrowingRunnable {

  void run() throws Exception;
}
