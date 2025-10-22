package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.Utils;
import com.gruelbox.transactionoutbox.testing.BaseTest;
import com.gruelbox.transactionoutbox.testing.InterfaceProcessor;
import com.gruelbox.transactionoutbox.testing.LatchListener;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  void runWithParentOtelSpan() throws Exception {
    var latch = new CountDownLatch(1);
    AtomicBoolean failedOnce = new AtomicBoolean();
    AtomicReference<SpanContext> remotedSpan = new AtomicReference<>();

    var txManager = TransactionManager.fromDataSource(dataSource);
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(txManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> {
                              if (failedOnce.compareAndSet(false, true)) {
                                throw new RuntimeException("Temporary failure");
                              }
                              remotedSpan.set(Span.current().getSpanContext());
                            }))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch).andThen(new OtelListener()))
            .blockAfterAttempts(2)
            .build();

    // Start a parent span, which should be propagated to the instantiator above
    Span parentSpan = otel.getTracer("parent-tracer").spanBuilder("parent-span").startSpan();
    String parentTraceId = null;
    try (Scope scope = parentSpan.makeCurrent()) {
      parentTraceId = Span.current().getSpanContext().getTraceId();
      txManager.inTransaction(() -> outbox.schedule(InterfaceProcessor.class).process(1, "1"));
    } finally {
      parentSpan.end();
    }

    // Wait for the job to complete
    withRunningFlusher(
        outbox,
        () -> {
          assertTrue(latch.await(10, TimeUnit.SECONDS));
        });

    var remotedSpanData =
        otelTesting.getSpans().stream()
            .filter(it -> it.getSpanId().equals(remotedSpan.get().getSpanId()))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No matching span"));

    // Check they ran with linked traces and the correct class/method/args
    assertTrue(
        remotedSpanData.getLinks().stream()
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No linked trace"))
            .getSpanContext()
            .getTraceId()
            .equals(parentTraceId));
    assertTrue(
        remotedSpanData
            .getName()
            .equals("com.gruelbox.transactionoutbox.testing.InterfaceProcessor.process"));
    assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg0")).equals("1"));
    assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg1")).equals("\"1\""));
  }

  @Test
  void runWithoutParentOtelSpan() throws Exception {
    var latch = new CountDownLatch(1);
    AtomicBoolean failedOnce = new AtomicBoolean();
    AtomicReference<SpanContext> remotedSpan = new AtomicReference<>();

    var txManager = TransactionManager.fromDataSource(dataSource);
    var outbox =
        TransactionOutbox.builder()
            .transactionManager(txManager)
            .persistor(Persistor.forDialect(connectionDetails().dialect()))
            .instantiator(
                Instantiator.using(
                    clazz ->
                        (InterfaceProcessor)
                            (foo, bar) -> {
                              if (failedOnce.compareAndSet(false, true)) {
                                throw new RuntimeException("Temporary failure");
                              }
                              remotedSpan.set(Span.current().getSpanContext());
                            }))
            .attemptFrequency(Duration.ofMillis(500))
            .listener(new LatchListener(latch).andThen(new OtelListener()))
            .blockAfterAttempts(2)
            .build();

    // Run with no parent span
    txManager.inTransaction(() -> outbox.schedule(InterfaceProcessor.class).process(1, "1"));

    // Wait for the job to complete
    withRunningFlusher(
        outbox,
        () -> {
          assertTrue(latch.await(10, TimeUnit.SECONDS));
        });

    var remotedSpanData =
        otelTesting.getSpans().stream()
            .filter(it -> it.getSpanId().equals(remotedSpan.get().getSpanId()))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No matching span"));

    // Check they ran with linked traces and the correct class/method/args
    assertFalse(remotedSpanData.getLinks().stream().findFirst().isPresent());
    assertTrue(
        remotedSpanData
            .getName()
            .equals("com.gruelbox.transactionoutbox.testing.InterfaceProcessor.process"));
    assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg0")).equals("1"));
    assertTrue(remotedSpanData.getAttributes().get(AttributeKey.stringKey("arg1")).equals("\"1\""));
  }

  /** Example {@link TransactionOutboxListener} to propagate traces */
  static class OtelListener implements TransactionOutboxListener {

    /** Serialises the current context into {@link Invocation#getSession()}. */
    @Override
    public Map<String, String> extractSession() {
      var result = new HashMap<String, String>();
      SpanContext spanContext = Span.current().getSpanContext();
      if (!spanContext.isValid()) {
        return null;
      }
      result.put("traceId", spanContext.getTraceId());
      result.put("spanId", spanContext.getSpanId());
      log.info("Extracted: {}", result);
      return result;
    }

    /**
     * Deserialises {@link Invocation#getSession()} and sets it as the current context so that any
     * new span started by the method we invoke will treat it as the parent span
     */
    @Override
    public void wrapInvocationAndInit(Invocator invocator) {
      Invocation inv = invocator.getInvocation();
      var spanBuilder =
          otel.getTracer("transaction-outbox")
              .spanBuilder(String.format("%s.%s", inv.getClassName(), inv.getMethodName()))
              .setNoParent();
      for (var i = 0; i < inv.getArgs().length; i++) {
        spanBuilder.setAttribute("arg" + i, Utils.stringify(inv.getArgs()[i]));
      }
      if (inv.getSession() != null) {
        var traceId = inv.getSession().get("traceId");
        var spanId = inv.getSession().get("spanId");
        if (traceId != null && spanId != null) {
          spanBuilder.addLink(
              SpanContext.createFromRemoteParent(
                  traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault()));
        }
      }
      var span = spanBuilder.startSpan();
      try (Scope scope = span.makeCurrent()) {
        invocator.runUnchecked();
      } finally {
        span.end();
      }
    }
  }
}
