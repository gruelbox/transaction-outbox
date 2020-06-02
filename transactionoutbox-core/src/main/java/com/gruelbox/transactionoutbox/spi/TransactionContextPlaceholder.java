package com.gruelbox.transactionoutbox.spi;

/**
 * Marker for {@link com.gruelbox.transactionoutbox.spi.Invocation} arguments holding transaction
 * context. These will be rehydrated with the real context type at runtime.
 */
public interface TransactionContextPlaceholder {}
