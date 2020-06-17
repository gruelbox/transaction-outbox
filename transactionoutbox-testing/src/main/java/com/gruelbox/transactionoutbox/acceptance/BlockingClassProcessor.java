package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingClassProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingClassProcessor.class);

  public static final List<String> PROCESSED = new CopyOnWriteArrayList<>();

  public void process(String itemId) {
    LOGGER.info("Processing work: {}", itemId);
    PROCESSED.add(itemId);
  }

  public void process(String itemId, JdbcTransaction tx) {
    LOGGER.info("Processing work: {}", itemId);
    PROCESSED.add(itemId);
  }
}
