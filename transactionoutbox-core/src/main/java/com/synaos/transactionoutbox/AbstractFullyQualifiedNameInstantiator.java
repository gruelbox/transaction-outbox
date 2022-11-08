package com.synaos.transactionoutbox;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractFullyQualifiedNameInstantiator implements Instantiator {

    @Override
    public final String getName(Class<?> clazz) {
        return clazz.getName();
    }

    @Override
    public final Object getInstance(String name) {
        log.debug("Getting class by name [{}]", name);
        return createInstance(Utils.uncheckedly(() -> Class.forName(name)));
    }

    protected abstract Object createInstance(Class<?> clazz);
}
