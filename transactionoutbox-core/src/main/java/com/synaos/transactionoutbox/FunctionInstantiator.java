package com.synaos.transactionoutbox;

import lombok.experimental.SuperBuilder;

import java.util.function.Function;

@SuperBuilder
class FunctionInstantiator extends AbstractFullyQualifiedNameInstantiator {

    private final Function<Class<?>, Object> fn;

    @Override
    public Object createInstance(Class<?> clazz) {
        return fn.apply(clazz);
    }
}
