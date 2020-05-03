package com.gruelbox.transactionoutbox;

import java.io.Reader;
import java.io.Writer;

/**
 * {@link Invocation} objects are inherently difficult to serialize safely since they are
 * unpredictably polymorphic. Allowing them to contain <em>any</em> type reference opens you
 * up to a host of code injection attacks. At the same time, allowing possibly-unstable types into
 * serialized {@link Invocation}s can result in compatibility issues, with still unprocessed entries
 * in the database containing older versions of your classes. To avoid this, it makes sense to
 * whitelist the types supported and restrict this whitelist to known-stable types such as
 * primitives and common JDK value types. {@link #createDefaultJsonSerializer()} provides exactly
 * this and is used by default. However, if you want to extend this list or use a different
 * serialization format, you can create your own implementation here, at your own risk.
 */
public interface InvocationSerializer {

  /**
   * Creates a locked-down serializer which supports a limited list of primitives and simple
   * JDK value types. Only the following are supported currently:
   *
   * <ul>
   *   <li>{@link Invocation} itself</i>
   *   <li>Primitive types such as {@code int} or {@code double} or the boxed equivalents
   *   <li>{@link String}
   *   <li>{@link java.util.Date}
   *   <li>The {@code java.time} classes:
   *       <ul>
   *         <li>{@link java.time.DayOfWeek}
   *         <li>{@link java.time.Duration}
   *         <li>{@link java.time.Instant}
   *         <li>{@link java.time.LocalDate}
   *         <li>{@link java.time.LocalDateTime}
   *         <li>{@link java.time.Month}
   *         <li>{@link java.time.MonthDay}
   *         <li>{@link java.time.Period}
   *         <li>{@link java.time.Year}
   *         <li>{@link java.time.YearMonth}
   *         <li>{@link java.time.ZoneOffset}
   *         <li>{@link java.time.DayOfWeek}
   *         <li>{@link java.time.temporal.ChronoUnit}
   *       </ul>
   *   <li>Arrays specifically typed to one of the above types.
   * </ul>
   *
   * @return The serializer.
   */
  static InvocationSerializer createDefaultJsonSerializer() {
    return new DefaultInvocationSerializer();
  }

  /**
   * Serializes an invocation to the supplied writer.
   *
   * @param invocation The invocation.
   * @param writer The writer.
   */
  void serializeInvocation(Invocation invocation, Writer writer);

  /**
   * Deserializes an invocation from the supplied reader.
   *
   * @param reader The reader.
   * @return The deserialized invocation.
   */
  Invocation deserializeInvocation(Reader reader);

}
