package com.gruelbox.transactionoutbox;

import java.time.Duration;

public class ExponentialBackOffStrategy implements NextRetryStrategy<ExponentialBackOffOptions> {

    @Override
    public Duration nextAttemptDelay(ExponentialBackOffOptions parameters, TransactionOutboxEntry entry) {

        return null;
    }
}
