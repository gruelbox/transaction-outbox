package com.gruelbox.transactionoutbox;

import java.io.StringReader;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("RedundantCast")
class TestDefaultInvocationSerializer {

  private static final String CLASS_NAME = "foo";
  private static final String METHOD_NAME = "bar";

  private DefaultInvocationSerializer serializer = new DefaultInvocationSerializer();

  @Test
  void testNoArgs() {
    check(new Invocation(String.class.getName(), "toString", new Class<?>[0], new Object[0]));
  }

  @Test
  void testArrays() {
    check(new Invocation(CLASS_NAME, METHOD_NAME, new Class<?>[]{int[].class},
        new Object[]{new int[]{1, 2, 3}}));
    check(new Invocation(CLASS_NAME, METHOD_NAME, new Class<?>[]{Integer[].class},
        new Object[]{new Integer[]{1, 2, 3}}));
    check(new Invocation(CLASS_NAME, METHOD_NAME, new Class<?>[]{String[].class},
        new Object[]{new String[]{"1", "2", "3"}}));
  }

  @Test
  void testPrimitives() {
    Class<?>[] primitives = {byte.class, short.class, int.class, long.class, float.class,
        double.class, boolean.class, char.class};
    Object[] values = {(byte) 1, (short) 2, 3, 4L, 1.23F, 1.23D, true, '-'};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testBoxedPrimitives() {
    Class<?>[] primitives = {Byte.class, Short.class, Integer.class, Long.class, Float.class,
        Double.class, Boolean.class, Character.class, String.class};
    Object[] values = {(Byte) (byte) 1, (Short) (short) 2, (Integer) 3, (Long) 4L, (Float) 1.23F,
        (Double) 1.23D, (Boolean) true, (Character) '-', "Foo"};
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
    Class<?>[] primitives = { Date.class };
    Object[] values = { new Date() };
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaTimeClasses() {
    Class<?>[] primitives = {Duration.class, Instant.class, LocalDate.class,
        LocalDateTime.class,
        MonthDay.class, Period.class, Year.class,
        YearMonth.class};
    Object[] values = {Duration.ofDays(1), Instant.now(), LocalDate.now(),
        LocalDateTime.now(), MonthDay.of(1, 1),
        Period.ofMonths(1), Year.now(), YearMonth.now()};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  void check(Invocation invocation) {
    Invocation deserialized = serdeser(invocation);
    Assertions.assertEquals(deserialized, serdeser(invocation));
    Assertions.assertEquals(invocation, deserialized);
  }

  Invocation serdeser(Invocation invocation) {
    String s = serializer.serialize(invocation);
    return serializer.deserialize(new StringReader(s));
  }
}
