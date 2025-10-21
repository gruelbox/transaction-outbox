package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Demonstrates wiring in OTEL. */
@Slf4j
public class TestOTEL extends BaseTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private static final OpenTelemetry otel = otelTesting.getOpenTelemetry();

  @Test
  void blah() throws InterruptedException {
    var latch = new CountDownLatch(1);
    AtomicReference<String> childTraceId = new AtomicReference<>();
    String parentTraceId = null;

    var txManager = TransactionManager.fromDataSource(dataSource);
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(txManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(
                Instantiator.using(
                    clazz ->
                        new InterfaceProcessor() {
                          @Override
                          public void process(int foo, String bar) {
                            // We should be running in the parent trace. We'll record the trace id
                            // and check afterwards
                            var span =
                                otel.getTracer("child-tracer")
                                    .spanBuilder("child-span")
                                    .startSpan();
                            try (Scope scope = span.makeCurrent()) {
                              childTraceId.set(span.getSpanContext().getTraceId());
                            } finally {
                              span.end();
                            }
                          }
                        }))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch).andThen(new OtelListener()))
            .blockAfterAttempts(2)
            .build();

    // Start a parent span, which should be propagated to the instantiator above
    Span parentSpan = otel.getTracer("parent-tracer").spanBuilder("parent-span").startSpan();
    try (Scope scope = parentSpan.makeCurrent()) {
      parentTraceId = parentSpan.getSpanContext().getTraceId();
      txManager.inTransaction(() -> outbox.schedule(InterfaceProcessor.class).process(1, "1"));
    } finally {
      parentSpan.end();
    }

    // Wait for the job to complete and check they ran in the same trace
    assertTrue(latch.await(10, TimeUnit.SECONDS));
    log.info("Child trace is {}", childTraceId.get());
    log.info("Parent trace is {}", parentTraceId);
    assertTrue(parentTraceId.equals(childTraceId.get()));
  }

  /** Example {@link TransactionOutboxListener} to propagate traces */
  static class OtelListener implements TransactionOutboxListener {

    /** Serialises the current context into {@link Invocation#getSession()}. */
    @Override
    public Map<String, String> extractSession() {
      var result = new HashMap<String, String>();
      TextMapSetter<HashMap<String, String>> setter = (carrier, k, v) -> carrier.put(k, v);
      otel.getPropagators().getTextMapPropagator().inject(Context.current(), result, setter);
      log.info("Extracted: {}", result);
      return result;
    }

    /**
     * Deserialises {@link Invocation#getSession()} and sets it as the current context so that any
     * new span started by the method we invoke will treat it as the parent span
     */
    @Override
    public void wrapInvocationAndInit(Invocator invocator) {
      Context remoteContext = deserializeRemoteContext(invocator.getInvocation());
      try (Scope scope = remoteContext.makeCurrent()) {
        invocator.runUnchecked();
      }
    }

    private Context deserializeRemoteContext(Invocation invocation) {
      var propagator = otel.getPropagators().getTextMapPropagator();
      var carrier = invocation.getSession();
      log.info("Received: {}", carrier);
      return propagator.extract(
          Context.root(),
          carrier,
          new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(Map<String, String> carrier) {
              return carrier.keySet();
            }

            @Override
            public String get(Map<String, String> carrier, String key) {
              return carrier.get(key);
            }
          });
    }
  }
}
