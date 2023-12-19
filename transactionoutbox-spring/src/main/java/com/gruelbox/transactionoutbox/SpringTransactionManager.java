package com.gruelbox.transactionoutbox;

import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @deprecated use {@link com.gruelbox.transactionoutbox.spring.SpringTransactionManager}.
 */
@Slf4j
@Service
@Deprecated(forRemoval = true)
public class SpringTransactionManager extends com.gruelbox.transactionoutbox.spring.SpringTransactionManager {

  @Delegate(types = { com.gruelbox.transactionoutbox.spring.SpringTransactionManager.class })
  private final com.gruelbox.transactionoutbox.spring.SpringTransactionManager delegate;

  @Autowired
  SpringTransactionManager(com.gruelbox.transactionoutbox.spring.SpringTransactionManager delegate) {
    super(null);
    this.delegate = delegate;
  }
}
