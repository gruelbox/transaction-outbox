package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;

/**
 * Implement on SPI component implementations such as {@link BaseTransactionManager} or {@link
 * com.gruelbox.transactionoutbox.Persistor} to receive notifications from other SPI components on
 * startup.
 */
@Beta
public interface InitializationEventSubscriber {

  /**
   * Called during startup of a {@code TransactionOutbox}. The implementer then may call back to the
   * supplied {@link InitializationEventBus} to register for events from other SPI components. This
   * will occur for all subscribers <strong>before</strong> invoking publishers to publish events.
   *
   * @param eventBus The event bus.
   */
  void onRegisterInitializationEvents(InitializationEventBus eventBus);
}
