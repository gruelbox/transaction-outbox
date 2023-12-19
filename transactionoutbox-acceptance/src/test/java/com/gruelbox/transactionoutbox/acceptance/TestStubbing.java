package com.gruelbox.transactionoutbox.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.gruelbox.transactionoutbox.*;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Checks that stubbing {@link TransactionOutbox} works cleanly. */
class TestStubbing {

  @Test
  void testStubbingWithThreadLocalContext() {
    StubThreadLocalTransactionManager transactionManager = new StubThreadLocalTransactionManager();
    TransactionOutbox outbox = createOutbox(transactionManager);

    Interface.invocations.clear();

    transactionManager.inTransaction(
        () -> {
          outbox
              .schedule(Interface.class)
              .doThing(1, "2", new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN});
          outbox.schedule(Interface.class).doThing(2, "3", new BigDecimal[] {});
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
    assertThat(Interface.invocations, contains(expected1, expected2, expected3, expected4));
  }

  @Test
  void testStubbingWithExplicitContextInvalidContext() {
    StubParameterContextTransactionManager<Context> transactionManager =
        new StubParameterContextTransactionManager<>(Context.class, () -> new Context(1L));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            transactionManager.inTransaction(
                tx -> outbox.schedule(Interface.class).doThing(1, new Context(2L))));
  }

  @Test
  void testStubbingWithExplicitContextPassingTransaction() {
    StubParameterContextTransactionManager<Context> transactionManager =
        new StubParameterContextTransactionManager<>(Context.class, () -> new Context(1L));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Interface.invocations.clear();

    transactionManager.inTransaction(tx -> outbox.schedule(Interface.class).doThing(1, tx));

    assertThat(Interface.invocations, hasSize(1));
    assertThat(Interface.invocations.get(0).get(0), equalTo(1));
    assertThat(Interface.invocations.get(0).get(1), isA(Transaction.class));
  }

  @Test
  void testStubbingWithExplicitContextPassingContext() {
    StubParameterContextTransactionManager<Context> transactionManager =
        new StubParameterContextTransactionManager<>(Context.class, () -> new Context(1L));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Interface.invocations.clear();

    transactionManager.inTransaction(
        tx -> outbox.schedule(Interface.class).doThing(1, (Context) tx.context()));

    assertThat(Interface.invocations, hasSize(1));
    assertThat(Interface.invocations.get(0).get(0), equalTo(1));
    assertThat(Interface.invocations.get(0).get(1), isA(Context.class));
  }

  private TransactionOutbox createOutbox(TransactionManager transactionManager) {
    return TransactionOutbox.builder()
        .instantiator(Instantiator.usingReflection())
        .persistor(StubPersistor.builder().build())
        .submitter(Submitter.withExecutor(Runnable::run))
        .transactionManager(transactionManager)
        .clockProvider(
            () ->
                Clock.fixed(
                    LocalDateTime.of(2020, 3, 1, 12, 0).toInstant(ZoneOffset.UTC),
                    ZoneOffset.UTC)) // Fix the clock
        .build();
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

    void doThing(@SuppressWarnings("SameParameterValue") int arg1, Transaction transaction) {
      assertThat(transaction, notNullValue());
      invocations.add(List.of(arg1, transaction));
    }

    void doThing(@SuppressWarnings("SameParameterValue") int arg1, Context context) {
      assertThat(context, notNullValue());
      invocations.add(List.of(arg1, context));
    }
  }

  @Value
  static class Context {
    long id;
  }
}
