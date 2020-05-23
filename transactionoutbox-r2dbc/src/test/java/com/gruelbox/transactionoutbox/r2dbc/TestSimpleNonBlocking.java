package com.gruelbox.transactionoutbox.r2dbc;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class TestSimpleNonBlocking {

  //  static {
  //    Async.init();
  //  }
  //
  //  @Test
  //  void test() {
  //
  //    Hooks.onOperatorDebug();
  //
  //    ConnectionFactoryWrapper connectionFactory =
  //        R2dbcRawTransactionManager.wrapConnectionFactory(
  //            ConnectionFactories.get("r2dbc:h2:mem://test:test@/test"));
  //    R2dbc r2dbc = new R2dbc(connectionFactory);
  //    R2dbcRawTransactionManager txMgr = new R2dbcRawTransactionManager(connectionFactory);
  //
  //    TransactionOutbox outbox =
  //        TransactionOutbox.builder()
  //            .transactionManager(txMgr)
  //            .persistor(R2dbcPersistor.forDialect(Dialect.H2))
  //            .submitter(Submitter.withExecutor(Runnable::run))
  //            .build();
  //
  //    r2dbc.inTransaction(handle -> handle.execute("CREATE TABLE test (val int)")).blockFirst();
  //
  //    log.info("Setup complete");
  //
  //    @SuppressWarnings("ConstantConditions")
  //    long count =
  //        r2dbc
  //            .inTransaction(
  //                h ->
  //                    Flux.concat(
  //                        h.execute("INSERT INTO test VALUES ($1)", 1),
  //                        h.execute("INSERT INTO test VALUES ($1)", 2),
  //                        h.execute("INSERT INTO test VALUES ($1)", 3),
  //                        Mono.fromRunnable(() -> log.info("Data inserted")),
  //                        Mono.fromCompletionStage(
  //                            outbox.schedule(Processor.class).process(connectionFromHandle(h),
  // 4))))
  //            .then(Mono.fromRunnable(() -> log.info("Scheduling complete")))
  //            .subscribeOn(Schedulers.parallel())
  //            .then(
  //                Mono.from(
  //                    r2dbc.inTransaction(
  //                        h ->
  //                            h.select("SELECT COUNT(*) FROM test")
  //                                .mapResult(result -> result.map((r, m) -> r.get(0,
  // Long.class))))))
  //            .block();
  //
  //    Assertions.assertEquals(4, count);
  //  }
  //
  //  private Connection connectionFromHandle(Handle handle) {
  //    try {
  //      Field field = Handle.class.getDeclaredField("connection");
  //      field.setAccessible(true);
  //      return (Connection) field.get(handle);
  //    } catch (NoSuchFieldException | IllegalAccessException e) {
  //      throw new RuntimeException(e);
  //    }
  //  }
  //
  //  @AllArgsConstructor
  //  static class Processor {
  //
  //    CompletableFuture<Void> process(Connection connection, int i) {
  //      return Mono.from(
  //              connection.createStatement("INSERT INTO test VALUES (?)").bind(0, i).execute())
  //          .then()
  //          .toFuture();
  //    }
  //  }
}
