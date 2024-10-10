package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.Dialect;
import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("outbox")
@Data
class TransactionOutboxProperties {
  private Duration repeatEvery;
  private boolean useJackson;
  private Duration attemptFrequency;
  private int blockAfterAttempts;
  private Dialect dialect;
}
