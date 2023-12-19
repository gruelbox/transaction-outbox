package com.gruelbox.transactionoutbox;

import lombok.experimental.Delegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @deprecated use {@link com.gruelbox.transactionoutbox.spring.SpringInstantiator}.
 */
@Service
@Deprecated(forRemoval = true)
public class SpringInstantiator implements Instantiator {

  @Delegate(types = { com.gruelbox.transactionoutbox.spring.SpringInstantiator.class })
  private final com.gruelbox.transactionoutbox.spring.SpringInstantiator delegate;

  @Autowired
  public SpringInstantiator(com.gruelbox.transactionoutbox.spring.SpringInstantiator delegate) {
    this.delegate = delegate;
  }

}
