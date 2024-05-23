package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.MissingOptionalDependencyException;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.TypeCache;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

/**
 * @author Ilya Viaznin
 */
@Slf4j
public abstract class AbstractProxyFactory {

    private final Objenesis objenesis = setupObjenesis();

    private final TypeCache<Class<?>> byteBuddyCache = setupByteBuddyCache();

    @SuppressWarnings({"unchecked", "cast"})
    public <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], T> processor) {
        if (clazz.isInterface()) {
            // Fastest - we can just proxy an interface directly
            return (T) Proxy.newProxyInstance(
                            clazz.getClassLoader(),
                            new Class[]{clazz},
                            (proxy, method, args) -> processor.apply(method, args));
        }
        else {
            var proxy = buildByteBuddyProxyClass(clazz);
            return constructProxy(clazz, processor, proxy);
        }
    }

    protected abstract <T> Callable<Class<?>> byteBuddyProxyCallable(Class<T> clazz);

    protected <T> T constructProxy(
            Class<T> clazz, BiFunction<Method, Object[], T> processor, Class<? extends T> proxy) {
        final T instance;
        if (hasDefaultConstructor(clazz)) {
            instance = Utils.uncheckedly(() -> proxy.getDeclaredConstructor().newInstance());
        }
        else {
            if (objenesis == null) {
                throw new MissingOptionalDependencyException("org.objenesis", "objenesis");
            }
            ObjectInstantiator<? extends T> instantiator = objenesis.getInstantiatorOf(proxy);
            instance = instantiator.newInstance();
        }
        Utils.uncheck(
                () -> {
                    var field = instance.getClass().getDeclaredField("handler");
                    field.set(
                            instance,
                            (InvocationHandler) (proxy1, method, args) -> processor.apply(method, args));
                });
        return instance;
    }

    protected void buddyCacheExistenceCheck() {
        Optional.ofNullable(byteBuddyCache)
                .orElseThrow(() -> new MissingOptionalDependencyException("net.bytebuddy", "byte-buddy"));
    }

    @SuppressWarnings({"unchecked", "cast"})
    protected <T> Class<? extends T> buildByteBuddyProxyClass(Class<T> clazz) {
        buddyCacheExistenceCheck();

        return (Class<? extends T>)
                byteBuddyCache.findOrInsert(
                        clazz.getClassLoader(),
                        clazz,
                        byteBuddyProxyCallable(clazz)
                );
    }

    private static boolean hasDefaultConstructor(Class<?> clazz) {
        try {
            clazz.getConstructor();
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private TypeCache<Class<?>> setupByteBuddyCache() {
        try {
            return new TypeCache<>(TypeCache.Sort.WEAK);
        } catch (NoClassDefFoundError error) {
            log.info(
                    "ByteBuddy is not on the classpath, so only interfaces can be used with transaction-outbox");
            return null;
        }
    }

    private ObjenesisStd setupObjenesis() {
        try {
            return new ObjenesisStd();
        } catch (NoClassDefFoundError error) {
            log.info(
                    "Objenesis is not on the classpath, so only interfaces or classes with default constructors can be used with transaction-outbox");
            return null;
        }
    }
}
