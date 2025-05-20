package com.gruelbox.transactionoutbox;

import java.time.Duration;

public interface NextRetryStrategy<T extends NextRetryStrategy.Options> {
    Duration nextAttemptDelay(T parameters, TransactionOutboxEntry entry);

    interface Options {
         String strategyClassName();
    }
}
