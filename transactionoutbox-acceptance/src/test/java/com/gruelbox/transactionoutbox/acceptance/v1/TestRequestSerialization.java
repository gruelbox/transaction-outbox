package com.gruelbox.transactionoutbox.acceptance.v1;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.DefaultInvocationSerializer;
import com.gruelbox.transactionoutbox.DefaultPersistor;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.acceptance.LatchListener;
import com.gruelbox.transactionoutbox.acceptance.TestUtils;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestRequestSerialization {

  /**
   * Ensures that we are serializing and deserializing any request before processing it. Otherwise
   * work could get processed locally successfully but fail when retried since the serialized
   * version of the request is not equivalent to the original.
   */
  @Test
  final void workAlwaysSerialized() throws Exception {
    TransactionManager transactionManager = simpleTxnManager();
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(transactionManager)
            .persistor(
                DefaultPersistor.builder()
                    .dialect(Dialect.H2)
                    .serializer(
                        DefaultInvocationSerializer.builder()
                            .serializableTypes(Set.of(Arg.class))
                            .build())
                    .build())
            .listener(new LatchListener(latch))
            .build();

    clearOutbox();

    Arg arg = new Arg();
    arg.hiddenData = "HIDDEN";
    arg.serializedData = "SERIALIZED";

    transactionManager.inTransaction(() -> outbox.schedule(ComplexProcessor.class).process(arg));
    assertTrue(latch.await(15, TimeUnit.SECONDS));
  }

  private TransactionManager simpleTxnManager() {
    return TransactionManager.fromConnectionDetails(
        "org.h2.Driver",
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE",
        "test",
        "test");
  }

  private void clearOutbox() {
    TestUtils.runSqlViaJdbc(simpleTxnManager(), "DELETE FROM TXNO_OUTBOX");
  }

  static class ComplexProcessor {

    public void process(Arg arg) {
      if (arg.hiddenData != null) {
        throw new IllegalStateException(
            "Running with state that could not possibly have been serialized");
      }
      if (!"SERIALIZED".equals(arg.serializedData)) {
        throw new IllegalStateException("No serialized state");
      }
    }
  }

  @Getter
  @Setter
  static class Arg {
    transient String hiddenData;
    String serializedData;
  }
}
