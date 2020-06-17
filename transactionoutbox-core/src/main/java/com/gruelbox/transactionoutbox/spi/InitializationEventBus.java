package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import java.util.function.Consumer;

/**
 * Passed to {@link InitializationEventPublisher}s and {@link InitializationEventSubscriber}s during
 * startup of a {@code TransactionOutbox} to offer them the opportunity to publish or receive
 * startup events.
 */
@Beta
public interface InitializationEventBus {

  /**
   * Called by {@link InitializationEventPublisher}s to publish an event to all registered {@link
   * InitializationEventSubscriber}s.
   *
   * @param eventType The event type. Will be dispatched to all callers of {@link #register(Class,
   *     Consumer)} who called with the same type.
   * @param event The event object. This does not have to be thread-safe or serializable, and may
   *     block (although this will slow down startup).
   * @param <T> The event type.
   */
  <T> void sendEvent(Class<T> eventType, T event);

  /**
   * Called by {@link InitializationEventSubscriber}s to register for dispatch on a corresponding
   * {@link #sendEvent(Class, Object)}.
   *
   * @param eventType The event type.
   * @param handler The callback.
   * @param <T> The event type.
   */
  <T> void register(Class<T> eventType, Consumer<T> handler);
}
