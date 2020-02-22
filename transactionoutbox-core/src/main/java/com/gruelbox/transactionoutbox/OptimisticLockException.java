package com.gruelbox.transactionoutbox;

/** Thrown when we attempt to update a record which has been modified by another thread. */
class OptimisticLockException extends Exception {}
