package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.CdiInstantiator;
import com.gruelbox.transactionoutbox.CdiTransactionManager;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.enterprise.inject.Produces;
import javax.sql.DataSource;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.ws.rs.core.Application;

public class ApplicationConfig extends Application {

  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();

    classes.add(BusinessService.class);

    return classes;
  }

  @Produces
  public TransactionManager cdiTransactionManager(
      DataSource datasource, TransactionSynchronizationRegistry tsr) throws SQLException {
    return CdiTransactionManager.create(datasource, tsr);
  }

  @Produces
  public TransactionOutbox transactionOutbox(
      TransactionManager transactionManager, RemoteCallService testProxy) {
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
