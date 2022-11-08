package com.gruelbox.transactionoutbox;

/**
 * Marker for {@link Invocation} arguments holding transaction context. These will be rehydrated
 * with the real context type at runtime.
 */
interface TransactionContextPlaceholder {

}
