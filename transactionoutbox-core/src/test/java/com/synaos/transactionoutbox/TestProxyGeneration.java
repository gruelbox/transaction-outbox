package com.synaos.transactionoutbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestProxyGeneration {

    private ProxyFactory proxyFactory;

    @BeforeEach
    void setUp() {
        proxyFactory = new ProxyFactory();
    }

    /**
     * Reflection
     */
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

    /**
     * ByteBuddy
     */
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

    /**
     * This fails without Objenesis.
     */
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
