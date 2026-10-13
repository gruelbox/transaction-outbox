package com.gruelbox.transactionoutbox.jackson3;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;

class TestCrossVersionSerialization {
  private final InvocationSerializer jackson2 =
      com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer.builder()
          .mapper(
              new com.fasterxml.jackson.databind.ObjectMapper()
                  .registerModule(new JavaTimeModule()))
          .build();

  private final InvocationSerializer jackson3 =
      JacksonInvocationSerializer.builder().mapper(JsonMapper.builder().build()).build();

  static Stream<Invocation> invocations() {
    return Stream.of(
        invocation(new Class<?>[] {int.class, String.class}, 1, "a"),
        invocation(new Class<?>[] {Color.class}, Color.GREEN),
        invocation(
            new Class<?>[] {
              Instant.class, LocalDate.class, LocalDateTime.class, Duration.class, Date.class
            },
            Instant.parse("2024-05-01T10:15:30.123456Z"),
            LocalDate.of(2024, 5, 1),
            LocalDateTime.of(2024, 5, 1, 10, 15, 30),
            Duration.ofSeconds(90),
            new Date(1714558530123L)),
        invocation(
            new Class<?>[] {List.class, Set.class, Map.class},
            List.of(1, 2, 3),
            Set.of("x"),
            Map.of("k", MonetaryAmount.ofGbp("1"))),
        invocation(
            new Class<?>[] {SerializationStressTestInput.class},
            new SerializationStressTestInput()),
        new Invocation(
            "c",
            "m",
            new Class<?>[] {String.class},
            new Object[] {"x"},
            Map.of("mdc", "1"),
            Map.of("session", "2")));
  }

  @ParameterizedTest
  @MethodSource("invocations")
  void writtenByJackson2ReadByJackson3(Invocation invocation) throws IOException {
    assertEquals(invocation, roundTrip(jackson2, jackson3, invocation));
  }

  @ParameterizedTest
  @MethodSource("invocations")
  void writtenByJackson3ReadByJackson2(Invocation invocation) throws IOException {
    assertEquals(invocation, roundTrip(jackson3, jackson2, invocation));
  }

  @Test
  void enumsAreWrittenByName() {
    StringWriter writer = new StringWriter();
    jackson3.serializeInvocation(invocation(new Class<?>[] {Color.class}, Color.GREEN), writer);
    assertTrue(writer.toString().contains("\"GREEN\""), writer.toString());
  }

  private static Invocation roundTrip(
      InvocationSerializer writer, InvocationSerializer reader, Invocation invocation)
      throws IOException {
    StringWriter out = new StringWriter();
    writer.serializeInvocation(invocation, out);
    return reader.deserializeInvocation(new StringReader(out.toString()));
  }

  private static Invocation invocation(Class<?>[] types, Object... args) {
    return new Invocation("c", "m", types, args);
  }
}
