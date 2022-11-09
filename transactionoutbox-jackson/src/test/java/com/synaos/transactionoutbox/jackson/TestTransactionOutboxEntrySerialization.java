package com.synaos.transactionoutbox.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.synaos.transactionoutbox.Invocation;
import com.synaos.transactionoutbox.TransactionOutboxEntry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTransactionOutboxEntrySerialization {

    @Test
    void test() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDefaultTyping(TransactionOutboxJacksonModule.typeResolver());
        objectMapper.registerModule(new TransactionOutboxJacksonModule());
        objectMapper.registerModule(new JavaTimeModule());

        var entry =
                TransactionOutboxEntry.builder()
                        .invocation(
                                new Invocation(
                                        "c",
                                        "m",
                                        new Class<?>[]{Map.class},
                                        new Object[]{
                                                Map.of(
                                                        "x", MonetaryAmount.ofGbp("200"),
                                                        "y", 3,
                                                        "z", List.of(1, 2, 3))
                                        },
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
}
