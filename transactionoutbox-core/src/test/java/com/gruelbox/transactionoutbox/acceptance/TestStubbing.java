package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.StubTransactionManager;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;

/**
 * Checks that stubbing {@link TransactionOutbox} works cleanly.
 */
@Slf4j
class TestStubbing {

  @Test
  void testStubbing() {
    StubTransactionManager transactionManager = StubTransactionManager.builder().build();
    TransactionOutbox outbox = TransactionOutbox.builder()
        .instantiator(Instantiator.usingReflection())
        .persistor(StubPersistor.builder().build())
        .submitter(Submitter.withExecutor(Runnable::run))
        .transactionManager(transactionManager)
        .clockProvider(() ->
            Clock.fixed(LocalDateTime.of(2020, 3, 1, 12, 0)
                .toInstant(ZoneOffset.UTC), ZoneOffset.UTC)) // Fix the clock
        .build();

    transactionManager.inTransaction(() -> {
      outbox.schedule(Interface.class).doThing(1, "2", new BigDecimal[] { BigDecimal.ONE, BigDecimal.TEN });
      outbox.schedule(Interface.class).doThing(2, "3", new BigDecimal[] { });
      outbox.schedule(Interface.class).doThing(3, null, null);
    });
    transactionManager.inTransaction(() -> outbox.schedule(Interface.class).doThing(4, null, null));

    Object expected1 = List.of(1, "2", List.of(BigDecimal.ONE, BigDecimal.TEN));
    Object expected2 = List.of(2, "3", List.of());
    List<Object> expected3 = new ArrayList<>();
    expected3.add(3);
    expected3.add(null);
    expected3.add(null);
    List<Object> expected4 = new ArrayList<>();
    expected4.add(4);
    expected4.add(null);
    expected4.add(null);
    MatcherAssert.assertThat(Interface.invocations, contains(expected1, expected2, expected3, expected4));
  }

  static class Interface {

    static List<List<Object>> invocations = new ArrayList<>();

    void doThing(int arg1, String arg2, BigDecimal[] arg3) {
      ArrayList<Object> args = new ArrayList<>();
      args.add(arg1);
      args.add(arg2);
      args.add(arg3 == null ? null : Arrays.asList(arg3));
      invocations.add(args);
    }
  }

}
