package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.Beta;
import lombok.Value;

@Beta
@Value
public class SerializableTypeRequired {
  Class<?> type;
}
