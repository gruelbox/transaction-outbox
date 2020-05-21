package com.gruelbox.transactionoutbox;

import java.time.Duration;

/**
 * Thrown when we attempt to schedule an invocation with a unique client id which has already been
 * used within {@link TransactionOutbox.TransactionOutboxBuilder#retentionThreshold(Duration)}.
 */
public class AlreadyScheduledException extends RuntimeException {}
