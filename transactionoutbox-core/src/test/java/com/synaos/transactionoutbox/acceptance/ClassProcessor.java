package com.synaos.transactionoutbox.acceptance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class ClassProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassProcessor.class);

    static final List<String> PROCESSED = new CopyOnWriteArrayList<>();

    void process(String itemId) {
        LOGGER.info("Processing work: {}", itemId);
        PROCESSED.add(itemId);
    }
}
