package com.gruelbox.transactionoutbox.acceptance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;

import com.ea.async.Async;
import com.gruelbox.transactionoutbox.Instantiator;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import com.gruelbox.transactionoutbox.jdbc.StubParameterContextJdbcTransactionManager;
import com.gruelbox.transactionoutbox.jdbc.StubThreadLocalJdbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.Transaction;
import com.gruelbox.transactionoutbox.spi.TransactionManager;
import java.math.BigDecimal;
import java.sql.Connection;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Checks that stubbing {@link TransactionOutbox} works cleanly. */
@Slf4j
class TestStubbingBlocking {

  static {
    Async.init();
  }

  @Test
  void testStubbingWithThreadLocalContext() {
    StubThreadLocalJdbcTransactionManager<Void, SimpleTransaction<Void>> transactionManager =
        new StubThreadLocalJdbcTransactionManager<>(
            () -> new SimpleTransaction<>(Mockito.mock(Connection.class), null));
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
    StubParameterContextJdbcTransactionManager<Context, SimpleTransaction<Context>>
        transactionManager =
            new StubParameterContextJdbcTransactionManager<>(
                Context.class,
                () -> new Context(1L),
                ctx -> new SimpleTransaction<>(Mockito.mock(Connection.class), ctx));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            transactionManager.inTransaction(
                tx -> outbox.schedule(Interface.class).doThing(1, new Context(2L))));
  }

  @Test
  void testStubbingWithExplicitContextPassingTransaction() {
    StubParameterContextJdbcTransactionManager<Context, SimpleTransaction<Context>>
        transactionManager =
            new StubParameterContextJdbcTransactionManager<>(
                Context.class,
                () -> new Context(1L),
                ctx -> new SimpleTransaction<>(Mockito.mock(Connection.class), ctx));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Interface.invocations.clear();

    transactionManager.inTransaction(tx -> outbox.schedule(Interface.class).doThing(1, tx));

    assertThat(Interface.invocations, hasSize(1));
    assertThat(Interface.invocations.get(0).get(0), equalTo(1));
    assertThat(Interface.invocations.get(0).get(1), isA(Transaction.class));
  }

  @Test
  void testStubbingWithExplicitContextPassingContext() {
    StubParameterContextJdbcTransactionManager<Context, SimpleTransaction<Context>>
        transactionManager =
            new StubParameterContextJdbcTransactionManager<>(
                Context.class,
                () -> new Context(1L),
                ctx -> new SimpleTransaction<>(Mockito.mock(Connection.class), ctx));
    TransactionOutbox outbox = createOutbox(transactionManager);

    Interface.invocations.clear();

    transactionManager.inTransaction(
        tx -> outbox.schedule(Interface.class).doThing(1, tx.context()));

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
      log.info("Complex method invoked");
      ArrayList<Object> args = new ArrayList<>();
      args.add(arg1);
      args.add(arg2);
      args.add(arg3 == null ? null : Arrays.asList(arg3));
      invocations.add(args);
    }

    void doThing(int arg1, Transaction transaction) {
      log.info("Transaction method invoked");
      assertThat(transaction, notNullValue());
      invocations.add(List.of(arg1, transaction));
    }

    void doThing(int arg1, Context context) {
      log.info("Context method invoked");
      assertThat(context, notNullValue());
      invocations.add(List.of(arg1, context));
    }
  }

  @Value
  private static class Context {
    long id;
  }
}
