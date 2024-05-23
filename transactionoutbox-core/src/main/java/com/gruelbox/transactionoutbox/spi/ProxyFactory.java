package com.gruelbox.transactionoutbox.spi;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.InvocationHandler;
import java.util.concurrent.Callable;

public class ProxyFactory extends AbstractProxyFactory {

    @Override
    protected <T> Callable<Class<?>> byteBuddyProxyCallable(Class<T> clazz) {
        return () -> new ByteBuddy()
                .subclass(clazz)
                .defineField("handler", InvocationHandler.class, Visibility.PUBLIC)
                .method(ElementMatchers.isDeclaredBy(clazz))
                .intercept(InvocationHandlerAdapter.toField("handler"))
                .make()
                .load(clazz.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    }
}
