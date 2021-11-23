package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.BiFunction;
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

public class ProxyFactory {

  private static final Objenesis objenesis = new ObjenesisStd();
  private static final TypeCache<Class<?>> byteBuddyCache = new TypeCache<>(Sort.WEAK);

  private static boolean hasDefaultConstructor(Class<?> clazz) {
    try {
      clazz.getConstructor();
      return true;
    } catch (NoSuchMethodException e) {
      return false;
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
    try {
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
    } catch (NoClassDefFoundError error) {
      throw new MissingOptionalDependencyException("net.bytebuddy", "byte-buddy");
    }
  }
}
