package com.gruelbox.transactionoutbox;


public class ExponentialBackOffOptions implements NextRetryStrategy.Options {

    private final int attemptFrequency;
    private  final int backOff;

    private ExponentialBackOffOptions(int attemptFrequency, int backOff) {
        this.attemptFrequency = attemptFrequency;
        this.backOff = backOff;
    }

    public static ExponentialBackOffOptions exponential(int attemptFrequency, int backOff){
        return new ExponentialBackOffOptions(attemptFrequency, backOff);
    }

    @Override
    public String strategyClassName() {
        return com.gruelbox.transactionoutbox.testing.ExponentialBackOffStrategy.class.getName();
    }
}
