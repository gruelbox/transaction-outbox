package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import java.util.function.Consumer;

@Beta
public interface InitializationEventBus {

  <T> void sendEvent(Class<T> eventType, T event);

  <T> void register(Class<T> eventType, Consumer<T> handler);
}
