package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailingInstantiator implements Instantiator {

  private final AtomicInteger attempts;

  public FailingInstantiator(AtomicInteger attempts) {
    this.attempts = attempts;
  }

  @Override
  public String getName(Class<?> clazz) {
    return "BEEF";
  }

  @Override
  public Object getInstance(String name) {
    if (!"BEEF".equals(name)) {
      throw new UnsupportedOperationException();
    }
    return new InterfaceProcessorImpl();
  }

  private final class InterfaceProcessorImpl implements InterfaceProcessor {

    @Override
    public void process(int foo, String bar) {
      log.info("Processing ({}, {})", foo, bar);
      if (attempts.incrementAndGet() < 3) {
        throw new RuntimeException("Temporary failure");
      }
      log.info("Processed ({}, {})", foo, bar);
    }

    @Override
    public CompletableFuture<Void> processAsync(int foo, String bar) {
      return InterfaceProcessor.super.processAsync(foo, bar);
    }
  }
}
