package com.gruelbox.transactionoutbox.jackson;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MonetaryAmount {

  private static final String GBP = "GBP";
  private static final BigDecimal BY_HUNDRED = BigDecimal.valueOf(100);

  public static final MonetaryAmount ZERO_GBP = new MonetaryAmount(BigDecimal.ZERO, GBP);
  public static final MonetaryAmount TEN_GBP = new MonetaryAmount(BigDecimal.TEN, GBP);
  public static final MonetaryAmount ONE_HUNDRED_GBP =
      new MonetaryAmount(BigDecimal.valueOf(100), GBP);

  private BigDecimal amount;

  private String currency;

  public static MonetaryAmount ofGbp(final String amount) {
    return new MonetaryAmount(new BigDecimal(amount), "GBP");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MonetaryAmount that = (MonetaryAmount) o;

    if (amount != null ? amount.compareTo(that.amount) != 0 : that.amount != null) return false;
    return Objects.equals(currency, that.currency);
  }

  @Override
  public int hashCode() {
    int result = amount != null ? amount.hashCode() : 0;
    result = 31 * result + (currency != null ? currency.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return currency + " " + amount.stripTrailingZeros().toPlainString();
  }
}
