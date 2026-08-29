package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.spi.ProxyFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestProxyGeneration {

  private ProxyFactory proxyFactory;

  @BeforeEach
  void setUp() {
    proxyFactory = new ProxyFactory();
  }

  /** Reflection */
  @Test
  void testReflection() {
    AtomicBoolean called = new AtomicBoolean();
    Interface proxy =
        proxyFactory.createProxy(
            Interface.class,
            (method, args) -> {
              called.set(true);
              return null;
            });
    proxy.doThing();
    assertTrue(called.get());
  }

  /** ByteBuddy */
  @Test
  void testByteBuddy() {
    AtomicBoolean called = new AtomicBoolean();
    Child proxy =
        proxyFactory.createProxy(
            Child.class,
            (method, args) -> {
              called.set(true);
              return null;
            });
    proxy.doThing();
    assertTrue(called.get());
  }

  /** This fails without Objenesis. */
  @Test
  void testObjensis() {
    AtomicBoolean called = new AtomicBoolean();
    Parent proxy =
        proxyFactory.createProxy(
            Parent.class,
            (method, args) -> {
              called.set(true);
              return null;
            });
    proxy.doThing();
    assertTrue(called.get());
  }

  @Test
  void testCallParentClassMethod() {
    AtomicBoolean called = new AtomicBoolean();
    AbstractServiceImpl serviceInstance = new AbstractServiceImpl(called);
    AbstractServiceImpl proxy =
        proxyFactory.createProxy(
            AbstractServiceImpl.class,
            (method, args) -> {
              Invocation invocation =
                  new Invocation(
                      AbstractServiceImpl.class.getName(),
                      method.getName(),
                      method.getParameterTypes(),
                      args);
              try {
                invocation.invoke(serviceInstance, new NoOpListener());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            });
    proxy.doThing();
    assertTrue(called.get());
  }

  interface Interface {
    void doThing();
  }

  static class Child {
    void doThing() {
      // No-op
    }
  }

  static class Parent {

    private final Child child;

    Parent(Child child) {
      this.child = child;
    }

    void doThing() {
      // No-op
    }
  }

  abstract static class AbstractService {
    private final AtomicBoolean called;

    protected AbstractService(AtomicBoolean called) {
      this.called = called;
    }

    public void doThing() {
      called.set(true);
    }
  }

  static class AbstractServiceImpl extends AbstractService {
    public AbstractServiceImpl(AtomicBoolean called) {
      super(called);
    }
  }

  static class NoOpListener implements TransactionOutboxListener {}
}
