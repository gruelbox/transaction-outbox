package com.synaos.transactionoutbox;

/**
 * A runnable... that throws.
 */
@FunctionalInterface
public interface ThrowingRunnable {

    void run() throws Exception;
}
