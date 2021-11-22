package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class TestProxyGeneration {

  /** Reflection */
  @Test
  void testReflection() {
    AtomicBoolean called = new AtomicBoolean();
    Interface proxy =
        Utils.createProxy(
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
        Utils.createProxy(
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
        Utils.createProxy(
            Parent.class,
            (method, args) -> {
              called.set(true);
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
}
