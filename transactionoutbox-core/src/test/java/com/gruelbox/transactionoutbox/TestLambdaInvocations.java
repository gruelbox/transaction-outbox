package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestLambdaInvocations {

  @Test
  final void lambdaInvocations() {
    var txm = new StubThreadLocalTransactionManager();
    var outbox =
        (TransactionOutboxImpl)
            TransactionOutbox.builder()
                .transactionManager(txm)
                .persistor(StubPersistor.builder().build())
                .instantiator(
                    Instantiator.using(
                        clazz -> {
                          throw new UnsupportedOperationException();
                        }))
                .submitter(Submitter.withExecutor(Runnable::run))
                .build();

    var i = 2;
    var j = 3;
    // TODO
    txm.inTransaction(() -> outbox.schedule(ins -> log.info("Foo {}, {}", i, j)));
  }
}
