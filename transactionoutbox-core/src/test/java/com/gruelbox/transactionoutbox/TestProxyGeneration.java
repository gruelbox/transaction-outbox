package com.gruelbox.transactionoutbox;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestProxyGeneration {

    /**
     * Reflection
     */
    @Test
    void testReflection() {
        AtomicBoolean called = new AtomicBoolean();
        Interface proxy = Utils.createProxy(Interface.class, (method, args) -> {
            called.set(true);
            return null;
        });
        proxy.doThing();
        assertTrue(called.get());
    }

    /**
     * CGLIB
     */
    @Test
    void testCGLIB() {
        AtomicBoolean called = new AtomicBoolean();
        Child proxy = Utils.createProxy(Child.class, (method, args) -> {
            called.set(true);
            return null;
        });
        proxy.doThing();
        assertTrue(called.get());
    }

    /**
     * This fails without Objenesis.
     */
    @Test
    void testObjensis() {
        AtomicBoolean called = new AtomicBoolean();
        Parent proxy = Utils.createProxy(Parent.class, (method, args) -> {
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
