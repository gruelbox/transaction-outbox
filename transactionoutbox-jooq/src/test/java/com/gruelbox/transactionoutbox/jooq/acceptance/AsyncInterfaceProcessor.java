package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.JooqTransaction;
import java.util.concurrent.CompletableFuture;
import org.jooq.Configuration;

interface AsyncInterfaceProcessor {

  CompletableFuture<Void> processAsync(int foo, String bar, Configuration context);

  CompletableFuture<Void> processAsync(int foo, String bar, JooqTransaction tx);
}
