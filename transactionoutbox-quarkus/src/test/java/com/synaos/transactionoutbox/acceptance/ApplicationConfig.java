package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.CdiInstantiator;
import com.synaos.transactionoutbox.QuarkusTransactionManager;
import com.synaos.transactionoutbox.Dialect;
import com.synaos.transactionoutbox.Persistor;
import com.synaos.transactionoutbox.TransactionOutbox;
import com.synaos.transactionoutbox.TransactionOutboxEntry;
import com.synaos.transactionoutbox.TransactionOutboxListener;

import java.util.HashSet;
import java.util.Set;
import javax.enterprise.inject.Produces;
import javax.ws.rs.core.Application;

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
