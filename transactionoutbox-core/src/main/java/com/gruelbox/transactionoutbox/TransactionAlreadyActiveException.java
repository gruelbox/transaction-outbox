package com.gruelbox.transactionoutbox;

/**
 * Thrown if a new transaction block is started when one is already active and the transaction
 * manage doesn't support connection suspending/parking.
 */
@SuppressWarnings("WeakerAccess")
public final class TransactionAlreadyActiveException extends RuntimeException {}
