package com.gruelbox.transactionoutbox;

import static java.lang.reflect.Modifier.isStatic;

import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.InitializationEventSubscriber;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
class InitializationEventBusImpl implements InitializationEventBus {

  private final Map<Class<?>, Set<Consumer>> subscribers = new HashMap<>();

  void subscribe(Object owner) {
    Arrays.stream(owner.getClass().getDeclaredFields())
        .filter(f -> !isStatic(f.getModifiers()))
        .forEach(
            f -> {
              f.setAccessible(true);
              try {
                Object value = f.get(owner);
                if (value instanceof InitializationEventSubscriber) {
                  log.debug("Adding subscriber to startup events: {}", value.getClass().getName());
                  ((InitializationEventSubscriber) value).onRegisterInitializationEvents(this);
                }
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
  }

  void publish(Object owner) {
    Arrays.stream(owner.getClass().getDeclaredFields())
        .filter(f -> !isStatic(f.getModifiers()))
        .forEach(
            f -> {
              f.setAccessible(true);
              try {
                Object value = f.get(owner);
                if (value instanceof InitializationEventPublisher) {
                  log.debug("Invoking startup event publisher: {}", value.getClass().getName());
                  ((InitializationEventPublisher) value).onPublishInitializationEvents(this);
                }
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void sendEvent(Class<T> eventType, T event) {
    Set<Consumer> consumers = subscribers.get(eventType);
    if (consumers != null) {
      consumers.forEach(
          subscriber -> {
            log.debug("Dispatching {} to {}", event, subscriber);
            subscriber.accept(event);
          });
    }
  }

  @Override
  public <T> void register(Class<T> eventType, Consumer<T> handler) {
    subscribers.computeIfAbsent(eventType, __ -> new HashSet<>()).add(handler);
  }
}
