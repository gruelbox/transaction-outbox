package com.gruelbox.transactionoutbox;

import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * Transaction manager which uses jOOQ's transaction management. In order to wire into JOOQ's
 * transaction lifecycle, a slightly convoluted construction process is required which involves
 * first creating a {@link JooqTransactionListener}, including it in the JOOQ {@link
 * org.jooq.Configuration} while constructing the root {@link DSLContext}, and then finally linking
 * the listener to the new {@link JooqTransactionManager}:
 *
 * <pre>
 * DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
 * DefaultConfiguration configuration = new DefaultConfiguration();
 * configuration.setConnectionProvider(connectionProvider);
 * configuration.setSQLDialect(SQLDialect.H2);
 * configuration.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider));
 * JooqTransactionListener listener = JooqTransactionManager.createListener();
 * configuration.set(listener);
 * DSLContext dsl = DSL.using(configuration);
 * return JooqTransactionManager.create(dsl, listener);</pre>
 */
public interface JooqTransactionManager extends TransactionManager {

  /**
   * Creates the {@link org.jooq.TransactionListener} to wire into the {@link DSLContext}. See
   * class-level documentation for more detail.
   *
   * @return The transaction listener.
   */
  static JooqTransactionListener createListener() {
    return new JooqTransactionListener();
  }

  /**
   * Creates a transaction manager which uses thread-local context. Attaches to the supplied {@link
   * JooqTransactionListener} to receive notifications of transactions starting and finishing on the
   * local thread so that {@link TransactionOutbox#schedule(Class)} can be called for methods that
   * don't explicitly inject a {@link Configuration}, e.g.:
   *
   * <pre>dsl.transaction(() -&gt; outbox.schedule(Foo.class).process("bar"));</pre>
   *
   * @param dslContext The DSL context.
   * @param listener The listener, linked to the DSL context.
   * @return The transaction manager.
   */
  static ThreadLocalContextTransactionManager create(
      DSLContext dslContext, JooqTransactionListener listener) {
    var result = new ThreadLocalJooqTransactionManager(dslContext);
    listener.setJooqTransactionManager(result);
    return result;
  }

  /**
   * Creates a transaction manager which uses explicitly-passed context, allowing multiple active
   * contexts in the current thread and contexts which are passed between threads. Requires a {@link
   * org.jooq.Configuration} for the transaction context or a {@link org.jooq.Transaction} to be
   * used to be passed any method called via {@link TransactionOutbox#schedule(Class)}. Example:
   *
   * <pre>
   * void doSchedule() {
   *   // ctx1 is used to write the request to the DB
   *   dsl.transaction(ctx1 -&gt; outbox.schedule(getClass()).process("bar", ctx1));
   * }
   *
   * // ctx2 is injected at run time
   * void process(String arg, org.jooq.Configuration ctx2) {
   *   ...
   * }</pre>
   *
   * <p>Or:
   *
   * <pre>
   * void doSchedule() {
   *   // tx1 is used to write the request to the DB
   *   transactionManager.inTransaction(tx1 -&gt; outbox.schedule(getClass()).process("bar", tx1));
   * }
   *
   * // tx2 is injected at run time
   * void process(String arg, Transaction tx2) {
   *   ...
   * }</pre>
   *
   * @param dslContext The DSL context.
   * @return The transaction manager.
   */
  static ParameterContextTransactionManager<Configuration> create(DSLContext dslContext) {
    return new DefaultJooqTransactionManager(dslContext);
  }
}
