package com.gruelbox.transactionoutbox.acceptance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.gruelbox.transactionoutbox.GuiceInstantiator;
import com.gruelbox.transactionoutbox.StubPersistor;
import com.gruelbox.transactionoutbox.StubThreadLocalTransactionManager;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates an alternative approach to using {@link TransactionOutbox} using binding annotations
 * to inject a remoted version of a service.
 */
@Slf4j
class TestGuiceBinding {

  /** The real service */
  @Inject MyService local;

  /** The remoted version */
  @Inject @Remote MyService remote;

  /** We need this to schedule the work */
  @Inject TransactionManager transactionManager;

  @Test
  void testProviderInjection() {
    AtomicBoolean processedWithRemote = new AtomicBoolean();
    Injector injector = Guice.createInjector(new DemoModule(processedWithRemote));
    injector.injectMembers(this);

    transactionManager.inTransaction(
        () -> {
          remote.process();
          log.info("Queued request");
        });

    assertTrue(processedWithRemote.get());
    assertTrue(local.processed.get());
  }

  /** The service we're going to remote */
  static class MyService {
    AtomicBoolean processed = new AtomicBoolean();

    void process() {
      processed.set(true);
      log.info("Processed local");
    }
  }

  /** Binding annotation for the remote version of the service. */
  @Target({ElementType.PARAMETER, ElementType.METHOD, ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface Remote {}

  /** Sets up the bindings */
  static final class DemoModule extends AbstractModule {

    private final AtomicBoolean processedWithRemote;

    DemoModule(AtomicBoolean processedWithRemote) {
      this.processedWithRemote = processedWithRemote;
    }

    @Provides
    @Singleton
    TransactionManager manager() {
      return new StubThreadLocalTransactionManager();
    }

    @Provides
    @Singleton
    TransactionOutbox outbox(Injector injector, TransactionManager transactionManager) {
      return TransactionOutbox.builder()
          .instantiator(GuiceInstantiator.builder().injector(injector).build())
          .persistor(StubPersistor.builder().build())
          .submitter(Submitter.withExecutor(Runnable::run))
          .transactionManager(transactionManager)
          .listener(
              new TransactionOutboxListener() {
                @Override
                public void success(TransactionOutboxEntry entry) {
                  processedWithRemote.set(true);
                }
              })
          .build();
    }

    @Provides
    @Remote
    @Singleton
    MyService remote(TransactionOutbox outbox) {
      return outbox.schedule(MyService.class);
    }

    @Provides
    @Singleton
    MyService local() {
      return new MyService();
    }
  }
}
