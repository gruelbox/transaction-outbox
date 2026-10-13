package com.gruelbox.transactionoutbox.jackson3;

import static org.junit.jupiter.api.Assertions.*;

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
public class TestTransactionOutboxEntrySerialization {

  @Test
  void test() {
    final ObjectMapper objectMapper = mapper();

    var entry =
        TransactionOutboxEntry.builder()
            .invocation(
                new Invocation(
                    "c",
                    "m",
                    new Class<?>[] {Map.class},
                    new Object[] {
                      Map.of(
                          "x", MonetaryAmount.ofGbp("200"),
                          "y", 3,
                          "z", List.of(1, 2, 3))
                    },
                    null,
                    null))
            .attempts(1)
            .blocked(true)
            .id("X")
            .description("Stuff")
            .nextAttemptTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))
            .uniqueRequestId("Y")
            .build();
    var s = objectMapper.writeValueAsString(entry);

    var deserialized = objectMapper.readValue(s, TransactionOutboxEntry.class);
    assertEquals(entry, deserialized);
  }

  @Test
  void testWithSessionAndMdc() {
    final ObjectMapper objectMapper = mapper();
    var entry =
        TransactionOutboxEntry.builder()
            .invocation(
                new Invocation(
                    "c",
                    "m",
                    new Class<?>[] {Map.class},
                    new Object[] {
                      Map.of(
                          "x", MonetaryAmount.ofGbp("200"),
                          "y", 3,
                          "z", List.of(1, 2, 3))
                    },
                    Map.of("a", "1"),
                    Map.of("b", "2", "c", "3")))
            .attempts(1)
            .blocked(true)
            .id("X")
            .description("Stuff")
            .nextAttemptTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))
            .uniqueRequestId("Y")
            .build();
    var s = objectMapper.writeValueAsString(entry);
    log.info(s);
    var deserialized = objectMapper.readValue(s, TransactionOutboxEntry.class);
    assertEquals(entry, deserialized);
  }

  private static tools.jackson.databind.ObjectMapper mapper() {
    return JsonMapper.builder()
        .setDefaultTyping(TransactionOutboxJacksonModule.typeResolver())
        .addModule(new TransactionOutboxJacksonModule())
        .build();
  }
}
