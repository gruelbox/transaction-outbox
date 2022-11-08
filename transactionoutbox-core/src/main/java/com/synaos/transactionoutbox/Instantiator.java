package com.synaos.transactionoutbox;

import java.util.function.Function;

/**
 * Provides callbacks for the creation and serialization of classes by {@link TransactionOutbox}.
 */
public interface Instantiator {

    /**
     * Creates an {@link Instantiator} which records the class name as its fully qualified name (e.g.
     * {@code com.gruelbox.example.EnterpriseBeanProxyFactoryFactory}) and instantiates instances
     * using reflection and a no-args constructor.
     *
     * <p>This is the default used by {@link TransactionOutbox} if nothing else is specified.
     *
     * @return A reflection instantiator
     */
    static Instantiator usingReflection() {
        return ReflectionInstantiator.builder().build();
    }

    /**
     * Creates an {@link Instantiator} which records the class name as its fully qualified name (e.g.
     * {@code com.gruelbox.example.EnterpriseBeanProxyFactoryFactory}) and instantiates instances
     * using the supplied function, which takes the fully qualified name and should return an
     * instance.
     *
     * <p>This is a good option to use with dependency injection frameworks such as Guice:
     *
     * <pre>TransactionOutbox outbox = TransactionOutbox.builder()
     * ...
     * .instantiator(Instantiator.using(injector::getInstance))
     * .build();</pre>
     *
     * @param fn A function to create an instance of the specified class.
     * @return A reflection instantiator
     */
    static Instantiator using(Function<Class<?>, Object> fn) {
        return FunctionInstantiator.builder().fn(fn).build();
    }

    /**
     * Provides the name of the specified class. This may be the classes fully-qualified name, or may
     * be an alias of some kind. This is up to the implementer.
     *
     * <p>Not using the actual class name can be useful in avoiding a case where queued tasks end up
     * referencing renamed classes following a refactor. It is also useful for DI frameworks such as
     * Spring DI, which use named bindings by default.
     *
     * @param clazz The class to get the name of.
     * @return The class name.
     */
    String getName(Class<?> clazz);

    /**
     * Requests an instance of the named class, where the "name" is whatever is returned by {@link
     * #getName(Class)}.
     *
     * <p>A common use-case for this method is to return a class from a DI framework such as Guice
     * (using an injected {code Injector}), but it is perfectly valid to simply instantiate the class
     * by name and populate its dependencies directly.
     *
     * @param name The class "name" as returned by {@link #getName(Class)}.
     * @return An instance of the class.
     */
    Object getInstance(String name);
}
