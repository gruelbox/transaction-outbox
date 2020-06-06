package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;

@Beta
public interface InitializationEventSubscriber {

  void onRegisterInitializationEvents(InitializationEventBus eventBus);
}
