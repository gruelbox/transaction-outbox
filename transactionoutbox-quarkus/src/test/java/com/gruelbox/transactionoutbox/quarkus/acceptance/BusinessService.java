package com.gruelbox.transactionoutbox.quarkus.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class BusinessService {
  private final DaoImpl dao;
  private final TransactionOutbox outbox;

  @Inject
  public BusinessService(DaoImpl dao, TransactionOutbox outbox) {
    this.dao = dao;
    this.outbox = outbox;
  }

  @Transactional
  public void writeSomeThingAndRemoteCall(String value, boolean throwException) {
    dao.writeSomethingIntoDatabase(value);
    RemoteCallService proxy = outbox.schedule(RemoteCallService.class);
    proxy.callRemote(throwException);
  }
}
