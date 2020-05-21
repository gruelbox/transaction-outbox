package com.gruelbox.transactionoutbox;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @deprecated Use {@link SpringInstantiator} and {@link SpringTransactionManager} as per the
 *     README.
 */
@Service
@Deprecated
public class SpringTransactionOutboxFactory {

  private final SpringTransactionManager springTransactionManager;
  private final SpringInstantiator springInstantiator;

  @Autowired
  public SpringTransactionOutboxFactory(
      SpringTransactionManager springTransactionManager, SpringInstantiator springInstantiator) {
    this.springTransactionManager = springTransactionManager;
    this.springInstantiator = springInstantiator;
  }

  public TransactionOutbox.TransactionOutboxBuilder create() {
    return TransactionOutbox.builder()
        .instantiator(springInstantiator)
        .transactionManager(springTransactionManager);
  }
}
