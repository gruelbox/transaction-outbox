package com.gruelbox.transactionoutbox;

import java.time.Duration;

public class DefaultRetryStrategy implements NextRetryStrategy<DefaultRetryOptions> {

    @Override
    public Duration nextAttemptDelay(DefaultRetryOptions parameters, TransactionOutboxEntry entry) {
        return parameters.getAttemptFrequency();
    }
}
