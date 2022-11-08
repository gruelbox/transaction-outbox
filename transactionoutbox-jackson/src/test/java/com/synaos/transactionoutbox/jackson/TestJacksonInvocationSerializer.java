package com.synaos.transactionoutbox.jackson;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.synaos.transactionoutbox.DefaultInvocationSerializer;
import com.synaos.transactionoutbox.Invocation;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("RedundantCast")
class TestJacksonInvocationSerializer {

  private JacksonInvocationSerializer underTest;

  private static final String CLASS_NAME = "foo";
  private static final String METHOD_NAME = "bar";

  @BeforeEach
  void beforeEach() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    underTest =
        JacksonInvocationSerializer.builder()
            .mapper(mapper)
            .defaultInvocationSerializer(
                DefaultInvocationSerializer.builder()
                    .serializableTypes(Set.of(Invocation.class))
                    .build())
            .build();
  }

  void check(Invocation invocation) {
    Invocation deserialized = serdeser(invocation);
    assertEquals(deserialized, serdeser(invocation));
    assertEquals(invocation, deserialized);
  }

  Invocation serdeser(Invocation invocation) {
    var writer = new StringWriter();
    underTest.serializeInvocation(invocation, writer);
    return underTest.deserializeInvocation(new StringReader(writer.toString()));
  }

  @Test
  void testNoArgs() {
    check(new Invocation(String.class.getName(), "toString", new Class<?>[0], new Object[0]));
  }

  @Test
  void testArrays() {
    check(
        new Invocation(
            CLASS_NAME,
            METHOD_NAME,
            new Class<?>[] {int[].class},
            new Object[] {new int[] {1, 2, 3}}));
    check(
        new Invocation(
            CLASS_NAME,
            METHOD_NAME,
            new Class<?>[] {Integer[].class},
            new Object[] {new Integer[] {1, 2, 3}}));
    check(
        new Invocation(
            CLASS_NAME,
            METHOD_NAME,
            new Class<?>[] {String[].class},
            new Object[] {new String[] {"1", "2", "3"}}));
  }

  @Test
  void testPrimitives() {
    Class<?>[] primitives = {
      short.class, int.class, long.class, float.class, double.class, boolean.class,
    };
    Object[] values = {(short) 2, 3, 4L, 1.23F, 1.23D, true};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testBoxedPrimitives() {
    Class<?>[] primitives = {
      Short.class, Integer.class, Long.class, Float.class, Double.class, Boolean.class, String.class
    };
    Object[] values = {
      (Short) (short) 2,
      (Integer) 3,
      (Long) 4L,
      (Float) 1.23F,
      (Double) 1.23D,
      (Boolean) true,
      "Foo"
    };
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaDateEnums() {
    Class<?>[] primitives = {DayOfWeek.class, Month.class, ChronoUnit.class};
    Object[] values = {DayOfWeek.FRIDAY, Month.APRIL, ChronoUnit.DAYS};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaUtilDate() {
    Class<?>[] primitives = {Date.class};
    Object[] values = {new Date()};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaTimeClasses() {
    Class<?>[] primitives = {
      Duration.class,
      Instant.class,
      LocalDate.class,
      LocalDateTime.class,
      MonthDay.class,
      Period.class,
      Year.class,
      YearMonth.class
    };
    Object[] values = {
      Duration.ofDays(1),
      Instant.now().truncatedTo(ChronoUnit.MICROS),
      LocalDate.now(),
      LocalDateTime.now(),
      MonthDay.of(1, 1),
      Period.ofMonths(1),
      Year.now(),
      YearMonth.now()
    };
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void deserializes_old_representation_correctly() {
    StringReader reader =
        new StringReader(
            "{\"c\":\"com.gruelbox.transactionoutbox.jackson.Service\",\"m\":\"parseDate\",\"p\":[\"String\"],\"a\":[{\"t\":\"String\",\"v\":\"2021-05-11\"}],\"x\":{\"REQUEST-ID\":\"someRequestId\"}}");
    Invocation invocation = underTest.deserializeInvocation(reader);
    assertEquals(
        new Invocation(
            "com.gruelbox.transactionoutbox.jackson.Service",
            "parseDate",
            new Class<?>[] {String.class},
            new Object[] {"2021-05-11"},
            Map.of("REQUEST-ID", "someRequestId")),
        invocation);
  }

  @Test
  void serializes_new_representation_stress_test() {
    Class<?>[] parameterTypes = new Class<?>[] {SerializationStressTestInput.class};
    Object[] args = new Object[] {new SerializationStressTestInput()};

    check(new Invocation(CLASS_NAME, METHOD_NAME, parameterTypes, args, null));
  }

  @Test
  void serializes_new_representation_list() {
    Class<?>[] parameterTypes = new Class<?>[] {List.class};
    Object[] args = new Object[] {List.of(MonetaryAmount.ofGbp("200"))};
    check(new Invocation(CLASS_NAME, METHOD_NAME, parameterTypes, args, null));
  }

  @Test
  void serializes_new_representation_set() {
    Class<?>[] parameterTypes = new Class<?>[] {Set.class};
    Object[] args = new Object[] {Set.of(MonetaryAmount.ofGbp("200"))};
    check(new Invocation(CLASS_NAME, METHOD_NAME, parameterTypes, args, null));
  }

  @Test
  void serializes_new_representation_map() {
    Class<?>[] parameterTypes = new Class<?>[] {Set.class};
    Object[] args = new Object[] {Map.of("investmentValue", MonetaryAmount.ofGbp("200"))};
    check(new Invocation(CLASS_NAME, METHOD_NAME, parameterTypes, args, null));
  }
}
