package com.gruelbox.transactionoutbox;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.time.Duration;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultRetryOptions implements NextRetryStrategy.Options {

    Duration attemptFrequency;

    @Override
    public String strategyClassName() {
        return DefaultRetryStrategy.class.getName();
    }

    public static DefaultRetryOptions withFrequency(Duration attemptFrequency){
        return new DefaultRetryOptions(attemptFrequency);
    }
}
