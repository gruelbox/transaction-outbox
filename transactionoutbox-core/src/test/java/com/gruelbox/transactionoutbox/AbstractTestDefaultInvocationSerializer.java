package com.gruelbox.transactionoutbox;

import java.io.StringReader;
import java.io.StringWriter;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("RedundantCast")
@Slf4j
abstract class AbstractTestDefaultInvocationSerializer {

  private static final String CLASS_NAME = "foo";
  private static final String METHOD_NAME = "bar";

  private final DefaultInvocationSerializer serializer;

  protected AbstractTestDefaultInvocationSerializer(Integer version) {
    this.serializer =
        DefaultInvocationSerializer.builder()
            .serializableTypes(Set.of(ExampleCustomEnum.class, ExampleCustomClass.class))
            .version(version)
            .build();
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
      byte.class,
      short.class,
      int.class,
      long.class,
      float.class,
      double.class,
      boolean.class,
      char.class
    };
    Object[] values = {(byte) 1, (short) 2, 3, 4L, 1.23F, 1.23D, true, '-'};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testBoxedPrimitives() {
    Class<?>[] primitives = {
      Byte.class,
      Short.class,
      Integer.class,
      Long.class,
      Float.class,
      Double.class,
      Boolean.class,
      Character.class,
      String.class
    };
    Object[] values = {
      (Byte) (byte) 1,
      (Short) (short) 2,
      (Integer) 3,
      (Long) 4L,
      (Float) 1.23F,
      (Double) 1.23D,
      (Boolean) true,
      (Character) '-',
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
  void testJavaDateEnumsNulls() {
    Class<?>[] primitives = {DayOfWeek.class, Month.class, ChronoUnit.class};
    Object[] values = {null, null, null};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaUtilDate() {
    Class<?>[] primitives = {Date.class, Date.class};
    Object[] values = {new Date(), null};
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
      YearMonth.class,
      ZonedDateTime.class
    };
    Object[] values = {
      Duration.ofDays(1),
      Instant.now(),
      LocalDate.now(),
      LocalDateTime.now(),
      MonthDay.of(1, 1),
      Period.ofMonths(1),
      Year.now(),
      YearMonth.now(),
      ZonedDateTime.now()
    };
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testJavaTimeClassesNulls() {
    Class<?>[] primitives = {
      Duration.class,
      Instant.class,
      LocalDate.class,
      LocalDateTime.class,
      MonthDay.class,
      Period.class,
      Year.class,
      YearMonth.class,
      ZonedDateTime.class
    };
    Object[] values = new Object[9];
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testCustomEnum() {
    Class<?>[] primitives = {ExampleCustomEnum.class, ExampleCustomEnum.class};
    Object[] values = {ExampleCustomEnum.ONE, ExampleCustomEnum.TWO};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testCustomEnumNulls() {
    Class<?>[] primitives = {ExampleCustomEnum.class};
    Object[] values = {null};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testCustomComplexClass() {
    Class<?>[] primitives = {ExampleCustomClass.class, ExampleCustomClass.class};
    Object[] values = {
      new ExampleCustomClass("Foo", "Bar"), new ExampleCustomClass("Bish", "Bash")
    };
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testMDC() {
    Class<?>[] primitives = {Integer.class};
    Object[] values = {1};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values, Map.of("A", "1", "B", "2")));
  }

  @Test
  void testUUID() {
    Class<?>[] primitives = {UUID.class};
    Object[] values = {UUID.randomUUID()};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  @Test
  void testUUIDNull() {
    Class<?>[] primitives = {UUID.class};
    Object[] values = {null};
    check(new Invocation(CLASS_NAME, METHOD_NAME, primitives, values));
  }

  void check(Invocation invocation) {
    Invocation deserialized = serdeser(invocation);
    Assertions.assertEquals(deserialized, serdeser(invocation));
    Assertions.assertEquals(invocation, deserialized);
  }

  Invocation serdeser(Invocation invocation) {
    var writer = new StringWriter();
    serializer.serializeInvocation(invocation, writer);
    log.info("Serialised as: {}", writer);
    return serializer.deserializeInvocation(new StringReader(writer.toString()));
  }

  enum ExampleCustomEnum {
    ONE,
    TWO
  }

  @Getter
  static class ExampleCustomClass {

    private final String arg1;
    private final String arg2;

    ExampleCustomClass(String arg1, String arg2) {
      this.arg1 = arg1;
      this.arg2 = arg2;
    }

    @Override
    public String toString() {
      return "ExampleCustomClass{" + "arg1='" + arg1 + '\'' + ", arg2='" + arg2 + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ExampleCustomClass that = (ExampleCustomClass) o;
      return Objects.equals(arg1, that.arg1) && Objects.equals(arg2, that.arg2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(arg1, arg2);
    }
  }
}
