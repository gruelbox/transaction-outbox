package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

@ApplicationScoped
public class BusinessService {
  private final DaoImpl dao;

  @Inject private TransactionOutbox outbox;

  @Inject
  public BusinessService(DaoImpl dao) {
    this.dao = dao;
  }

  @Transactional
  public void writeSomeThingAndRemoteCall(String value, boolean throwException) {
    dao.writeSomethingIntoDatabase(value);
    RemoteCallService proxy = outbox.schedule(RemoteCallService.class);
    proxy.callRemote(throwException);
  }
}
