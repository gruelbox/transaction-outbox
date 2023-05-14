package com.gruelbox.transactionoutbox.jackson;

import java.util.Map;
import java.util.Set;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

@Getter
@Setter
@Jacksonized
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class SerializationStressTestInput {
  private boolean enabled = false;
  private MonetaryAmount amount = MonetaryAmount.ONE_HUNDRED_GBP;
  private Set<String> investments = Set.of("investment1", "investment2", "investment3");
  private Map<String, MonetaryAmount> investmentAmounts =
      Map.of(
          "investment1",
          MonetaryAmount.ofGbp("33"),
          "investment2",
          MonetaryAmount.ofGbp("34"),
          "investment3",
          MonetaryAmount.ofGbp("33"));
}
