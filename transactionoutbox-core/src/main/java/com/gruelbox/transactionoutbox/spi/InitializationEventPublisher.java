package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;

@Beta
public interface InitializationEventPublisher {

  void onPublishInitializationEvents(InitializationEventBus eventBus);
}
