package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.TypeCache;
import net.bytebuddy.TypeCache.Sort;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

@Slf4j
public class ProxyFactory {

  private final Objenesis objenesis = setupObjenesis();
  private final TypeCache<Class<?>> byteBuddyCache = setupByteBuddyCache();

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
      return new TypeCache<>(Sort.WEAK);
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

  @SuppressWarnings({"unchecked", "cast"})
  <T> T createProxy(Class<T> clazz, BiFunction<Method, Object[], T> processor) {
    if (clazz.isInterface()) {
      // Fastest - we can just proxy an interface directly
      return (T)
          Proxy.newProxyInstance(
              clazz.getClassLoader(),
              new Class[] {clazz},
              (proxy, method, args) -> processor.apply(method, args));
    } else {
      Class<? extends T> proxy = buildByteBuddyProxyClass(clazz);
      return constructProxy(clazz, processor, proxy);
    }
  }

  private <T> T constructProxy(
      Class<T> clazz, BiFunction<Method, Object[], T> processor, Class<? extends T> proxy) {
    final T instance;
    if (hasDefaultConstructor(clazz)) {
      instance = Utils.uncheckedly(() -> proxy.getDeclaredConstructor().newInstance());
    } else {
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

  @SuppressWarnings({"unchecked", "cast"})
  private <T> Class<? extends T> buildByteBuddyProxyClass(Class<T> clazz) {
    if (byteBuddyCache == null) {
      throw new MissingOptionalDependencyException("net.bytebuddy", "byte-buddy");
    }
    return (Class<? extends T>)
        byteBuddyCache.findOrInsert(
            clazz.getClassLoader(),
            clazz,
            () ->
                new ByteBuddy()
                    .subclass(clazz)
                    .defineField("handler", InvocationHandler.class, Visibility.PUBLIC)
                    .method(ElementMatchers.isDeclaredBy(clazz))
                    .intercept(InvocationHandlerAdapter.toField("handler"))
                    .make()
                    .load(clazz.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded());
  }
}
