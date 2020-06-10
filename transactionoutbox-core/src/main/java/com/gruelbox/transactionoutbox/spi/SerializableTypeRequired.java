package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import lombok.Value;

/**
 * A standard event intended to be fired by {@link BaseTransactionManager} implementations
 * implementing {@link InitializationEventPublisher}.
 *
 * <p>Registers the need for a type to appear in serialized invocations.
 */
@Beta
@Value
public class SerializableTypeRequired {
  /**
   * @return The type required to be serializable.
   */
  Class<?> type;
}
