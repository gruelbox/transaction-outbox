package com.gruelbox.transactionoutbox.acceptance;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.gruelbox.transactionoutbox.GuiceInstantiator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class TestGuiceInstantiator {

  @Test
  void testInjection() {
    Injector injector = Guice.createInjector();
    GuiceInstantiator guiceInstantiator = GuiceInstantiator.builder().injector(injector).build();
    Object instance = guiceInstantiator.getInstance(Parent.class.getName());
    MatcherAssert.assertThat(instance, Matchers.isA(Parent.class));
  }

  static final class Child {}

  static final class Parent {

    private final Child child;

    @Inject
    Parent(Child child) {
      this.child = child;
    }
  }
}
