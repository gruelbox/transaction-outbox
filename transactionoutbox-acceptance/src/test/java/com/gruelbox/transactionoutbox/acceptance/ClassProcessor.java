package com.gruelbox.transactionoutbox.acceptance;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingClassProcessor.class);

  public static final List<String> PROCESSED = new CopyOnWriteArrayList<>();

  public void process(String itemId) {
    LOGGER.info("Processing work: {}", itemId);
    PROCESSED.add(itemId);
  }

  public CompletableFuture<Void> processAsync(String itemId) {
    process(itemId);
    return CompletableFuture.completedFuture(null);
  }
}
