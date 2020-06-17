package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;

/**
 * Implement on SPI component implementations such as {@link BaseTransactionManager} or {@link
 * com.gruelbox.transactionoutbox.Persistor} to publish information to other SPI components on
 * startup.
 */
@Beta
public interface InitializationEventPublisher {

  /**
   * Called during startup of a {@code TransactionOutbox}. The implementer then may call back to the
   * supplied {@link InitializationEventBus} to publish information to other SPI components.
   *
   * @param eventBus The event bus.
   */
  void onPublishInitializationEvents(InitializationEventBus eventBus);
}
