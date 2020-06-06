package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Instantiator;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingInstantiator implements Instantiator {

  @Override
  public String getName(Class<?> clazz) {
    return clazz.getName();
  }

  @Override
  public Object getInstance(String name) {
    if (InterfaceProcessor.class.getName().equals(name)) {
      return new InterfaceProcessorImpl();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static final class InterfaceProcessorImpl implements InterfaceProcessor {

    @Override
    public void process(int foo, String bar) {
      log.info("Processing ({}, {})", foo, bar);
    }

    @Override
    public CompletableFuture<Void> processAsync(int foo, String bar) {
      return InterfaceProcessor.super.processAsync(foo, bar);
    }
  }
}
