package com.gruelbox.transactionoutbox.quarkus.acceptance;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.quarkus.CdiInstantiator;
import com.gruelbox.transactionoutbox.quarkus.QuarkusTransactionManager;
import jakarta.enterprise.inject.Produces;
import jakarta.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

public class ApplicationConfig extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();

    classes.add(BusinessService.class);

    return classes;
  }

  @Produces
  public TransactionOutbox transactionOutbox(
      QuarkusTransactionManager transactionManager, RemoteCallService testProxy) {
    return TransactionOutbox.builder()
        .instantiator(CdiInstantiator.create())
        .blockAfterAttempts(1)
        .listener(
            new TransactionOutboxListener() {
              @Override
              public void blocked(TransactionOutboxEntry entry, Throwable cause) {
                block(testProxy);
              }
            })
        .transactionManager(transactionManager)
        .persistor(Persistor.forDialect(Dialect.H2))
        .build();
  }

  private void block(RemoteCallService testProxy) {
    testProxy.block();
  }
}
