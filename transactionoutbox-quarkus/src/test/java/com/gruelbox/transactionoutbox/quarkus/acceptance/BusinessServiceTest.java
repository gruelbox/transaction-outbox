package com.gruelbox.transactionoutbox.quarkus.acceptance;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class BusinessServiceTest {
  @Inject private BusinessService res;

  @Inject private RemoteCallService remoteCall;

  @Inject private DaoImpl dao;

  @BeforeEach
  void purgeDatabase() {
    dao.purge();
    remoteCall.setCalled(false);
    remoteCall.setBlocked(false);
  }

  @Test
  void writeOperationAndRemoteCallOK() throws Exception {
    Assertions.assertFalse(remoteCall.isCalled());

    res.writeSomeThingAndRemoteCall("toto", false);

    Thread.sleep(1000);

    Assertions.assertTrue(remoteCall.isCalled());
    Assertions.assertFalse(dao.getFromDatabase().isEmpty());
  }

  @Test
  void writeOperationOkButRemoteCallErrorShouldBlockRemoteCall() throws Exception {
    Assertions.assertFalse(remoteCall.isCalled());

    res.writeSomeThingAndRemoteCall("toto", true);

    Thread.sleep(1000);

    Assertions.assertFalse(remoteCall.isCalled());
    Assertions.assertFalse(dao.getFromDatabase().isEmpty());
    Assertions.assertTrue(remoteCall.isBlocked());
  }

  @Test
  void transactionRollbackSoRemoteCallShouldNotBeMade() throws Exception {
    Assertions.assertFalse(remoteCall.isCalled());
    try {
      res.writeSomeThingAndRemoteCall("error", false);
      Assertions.fail("Should not happen");
    } catch (RuntimeException e) {
      Assertions.assertEquals("Persistence error", e.getMessage());
    }

    Thread.sleep(1000);

    Assertions.assertFalse(remoteCall.isCalled());
    Assertions.assertTrue(dao.getFromDatabase().isEmpty());
    Assertions.assertFalse(remoteCall.isBlocked());
  }
}
