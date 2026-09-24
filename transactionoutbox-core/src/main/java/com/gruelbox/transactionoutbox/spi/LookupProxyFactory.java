package com.gruelbox.transactionoutbox.spi;

import lombok.SneakyThrows;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationHandler;
import java.util.concurrent.Callable;

/**
 * Proxy factory that uses lookup load method in bytebuddy
 * <p>
 * Use for JDK 17+
 *
 * @author Ilya Viaznin
 */
public class LookupProxyFactory extends AbstractProxyFactory {

    @Override
    @SneakyThrows
    protected <T> Callable<Class<?>> byteBuddyProxyCallable(Class<T> clazz) {
        var lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
        return () -> new ByteBuddy()
                .subclass(clazz)
                .defineField("handler", InvocationHandler.class, Visibility.PUBLIC)
                .method(ElementMatchers.isDeclaredBy(clazz))
                .intercept(InvocationHandlerAdapter.toField("handler"))
                .make()
                .load(clazz.getClassLoader(), ClassLoadingStrategy.UsingLookup.of(lookup))
                .getLoaded();
    }
}
