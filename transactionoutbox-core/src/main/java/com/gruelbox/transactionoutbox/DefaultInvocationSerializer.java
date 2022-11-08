package com.gruelbox.transactionoutbox;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * A locked-down serializer which supports a limited list of primitives and simple JDK value types.
 * Only the following are supported:
 *
 * <ul>
 *   <li>{@link Invocation} itself
 *   <li>Primitive types such as {@code int} or {@code double} or the boxed equivalents
 *   <li>{@link String}
 *   <li>{@link Date}
 *   <li>{@link UUID}
 *   <li>The {@code java.time} classes:
 *       <ul>
 *         <li>{@link DayOfWeek}
 *         <li>{@link Duration}
 *         <li>{@link Instant}
 *         <li>{@link LocalDate}
 *         <li>{@link LocalDateTime}
 *         <li>{@link Month}
 *         <li>{@link MonthDay}
 *         <li>{@link Period}
 *         <li>{@link Year}
 *         <li>{@link YearMonth}
 *         <li>{@link ZoneOffset}
 *         <li>{@link DayOfWeek}
 *         <li>{@link ChronoUnit}
 *       </ul>
 *   <li>Arrays specifically typed to one of the above types
 *   <li>Any types specifically passed in, which must be GSON compatible.
 * </ul>
 */
@Slf4j
public final class DefaultInvocationSerializer implements InvocationSerializer {

    private final Gson gson;

    @Builder
    DefaultInvocationSerializer(Set<Class<?>> serializableTypes, Integer version) {
        this.gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                Invocation.class,
                                new InvocationJsonSerializer(
                                        serializableTypes == null ? Set.of() : serializableTypes,
                                        version == null ? 2 : version))
                        .registerTypeAdapter(Date.class, new UtcDateTypeAdapter())
                        .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeTypeAdapter())
                        .registerTypeAdapter(Instant.class, new InstantTypeAdapter())
                        .registerTypeAdapter(Duration.class, new DurationTypeAdapter())
                        .registerTypeAdapter(LocalDate.class, new LocalDateTypeAdapter())
                        .registerTypeAdapter(MonthDay.class, new MonthDayTypeAdapter())
                        .registerTypeAdapter(Period.class, new PeriodTypeAdapter())
                        .registerTypeAdapter(Year.class, new YearTypeAdapter())
                        .registerTypeAdapter(YearMonth.class, new YearMonthAdapter())
                        .excludeFieldsWithModifiers(Modifier.TRANSIENT, Modifier.STATIC)
                        .create();
    }

    @Override
    public void serializeInvocation(Invocation invocation, Writer writer) {
        try {
            gson.toJson(invocation, writer);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot serialize " + invocation, e);
        }
    }

    @Override
    public Invocation deserializeInvocation(Reader reader) {
        return gson.fromJson(reader, Invocation.class);
    }

    private static final class InvocationJsonSerializer
            implements JsonSerializer<Invocation>, JsonDeserializer<Invocation> {

        private final int version;
        private final Map<Class<?>, String> classToName = new HashMap<>();
        private final Map<String, Class<?>> nameToClass = new HashMap<>();

        InvocationJsonSerializer(Set<Class<?>> serializableClasses, int version) {
            this.version = version;
            addClassPair(byte.class, "byte");
            addClassPair(short.class, "short");
            addClassPair(int.class, "int");
            addClassPair(long.class, "long");
            addClassPair(float.class, "float");
            addClassPair(double.class, "double");
            addClassPair(boolean.class, "boolean");
            addClassPair(char.class, "char");

            addClass(Byte.class);
            addClass(Short.class);
            addClass(Integer.class);
            addClass(Long.class);
            addClass(Float.class);
            addClass(Double.class);
            addClass(Boolean.class);
            addClass(Character.class);

            addClass(BigDecimal.class);
            addClass(String.class);
            addClass(Date.class);
            addClass(UUID.class);

            addClass(DayOfWeek.class);
            addClass(Duration.class);
            addClass(Instant.class);
            addClass(LocalDate.class);
            addClass(LocalDateTime.class);
            addClass(Month.class);
            addClass(MonthDay.class);
            addClass(Period.class);
            addClass(Year.class);
            addClass(YearMonth.class);
            addClass(ZoneOffset.class);
            addClass(DayOfWeek.class);
            addClass(ChronoUnit.class);

            addClass(Transaction.class);
            addClassPair(TransactionContextPlaceholder.class, "TransactionContext");

            serializableClasses.forEach(clazz -> addClassPair(clazz, clazz.getName()));
        }

        private void addClass(Class<?> clazz) {
            addClassPair(clazz, clazz.getSimpleName());
        }

        private void addClassPair(Class<?> clazz, String name) {
            classToName.put(clazz, name);
            nameToClass.put(name, clazz);
            String arrayClassName = toArrayClassName(clazz);
            Class<?> arrayClass = toClass(clazz.getClassLoader(), arrayClassName);
            classToName.put(arrayClass, arrayClassName);
            nameToClass.put(arrayClassName, arrayClass);
        }

        private String toArrayClassName(Class<?> clazz) {
            if (clazz.isArray()) {
                return "[" + clazz.getName();
            } else if (clazz == boolean.class) {
                return "[Z";
            } else if (clazz == byte.class) {
                return "[B";
            } else if (clazz == char.class) {
                return "[C";
            } else if (clazz == double.class) {
                return "[D";
            } else if (clazz == float.class) {
                return "[F";
            } else if (clazz == int.class) {
                return "[I";
            } else if (clazz == long.class) {
                return "[J";
            } else if (clazz == short.class) {
                return "[S";
            } else {
                return "[L" + clazz.getName() + ";";
            }
        }

        private Class<?> toClass(ClassLoader classLoader, String name) {
            try {
                return classLoader != null ? Class.forName(name, false, classLoader) : Class.forName(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Cannot determine array type for "
                                + name
                                + " using "
                                + (classLoader == null ? "root classloader" : "base classloader"),
                        e);
            }
        }

        @Override
        public JsonElement serialize(Invocation src, Type typeOfSrc, JsonSerializationContext context) {
            if (version == 1) {
                log.warn("Serializing as deprecated version {}", version);
                return serializeV1(src, typeOfSrc, context);
            }
            JsonObject obj = new JsonObject();
            obj.addProperty("c", src.getClassName());
            obj.addProperty("m", src.getMethodName());
            JsonArray params = new JsonArray();
            JsonArray args = new JsonArray();
            int i = 0;
            for (Class<?> parameterType : src.getParameterTypes()) {
                params.add(nameForClass(parameterType));
                Object arg = src.getArgs()[i];
                if (arg == null) {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.add("t", null);
                    jsonObject.add("v", null);
                    args.add(jsonObject);
                } else {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("t", nameForClass(arg.getClass()));
                    jsonObject.add("v", context.serialize(arg));
                    args.add(jsonObject);
                }
                i++;
            }
            obj.add("p", params);
            obj.add("a", args);
            obj.add("x", context.serialize(src.getMdc()));
            return obj;
        }

        JsonElement serializeV1(Invocation src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject obj = new JsonObject();
            obj.addProperty("c", src.getClassName());
            obj.addProperty("m", src.getMethodName());
            JsonArray params = new JsonArray();
            int i = 0;
            for (Class<?> parameterType : src.getParameterTypes()) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("t", nameForClass(parameterType));
                jsonObject.add("v", context.serialize(src.getArgs()[i]));
                params.add(jsonObject);
                i++;
            }
            obj.add("p", params);
            obj.add("x", context.serialize(src.getMdc()));
            return obj;
        }

        @Override
        public Invocation deserialize(
                JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {

            JsonObject jsonObject = json.getAsJsonObject();
            String className = jsonObject.get("c").getAsString();
            String methodName = jsonObject.get("m").getAsString();

            JsonArray jsonParams = jsonObject.get("p").getAsJsonArray();
            Class<?>[] params = new Class<?>[jsonParams.size()];
            for (int i = 0; i < jsonParams.size(); i++) {
                JsonElement param = jsonParams.get(i);
                if (param.isJsonObject()) {
                    // For backwards compatibility
                    params[i] = classForName(param.getAsJsonObject().get("t").getAsString());
                } else {
                    params[i] = classForName(param.getAsString());
                }
            }

            JsonElement argsElement = jsonObject.get("a");
            if (argsElement == null) {
                // For backwards compatibility
                argsElement = jsonObject.get("p");
            }
            JsonArray jsonArgs = argsElement.getAsJsonArray();
            Object[] args = new Object[jsonArgs.size()];
            for (int i = 0; i < jsonArgs.size(); i++) {
                JsonElement arg = jsonArgs.get(i);
                JsonElement argType = arg.getAsJsonObject().get("t");
                if (argType != null) {
                    JsonElement argValue = arg.getAsJsonObject().get("v");
                    Class<?> argClass = classForName(argType.getAsString());
                    try {
                        args[i] = context.deserialize(argValue, argClass);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to deserialize arg [" + argValue + "] of type [" + argType + "]", e);
                    }
                }
            }
            Map<String, String> mdc = context.deserialize(jsonObject.get("x"), Map.class);

            return new Invocation(className, methodName, params, args, mdc);
        }

        private Class<?> classForName(String name) {
            var clazz = nameToClass.get(name);
            if (clazz == null) {
                throw new IllegalArgumentException("Cannot deserialize class - not found: " + name);
            }
            return clazz;
        }

        private String nameForClass(Class<?> clazz) {
            var name = classToName.get(clazz);
            if (name == null) {
                throw new IllegalArgumentException(
                        "Cannot serialize class - not found: " + clazz.getName());
            }
            return name;
        }
    }

    static final class LocalDateTimeTypeAdapter extends TypeAdapter<LocalDateTime> {

        @Override
        public void write(JsonWriter out, LocalDateTime value) throws IOException {
            out.value(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }

        @Override
        public LocalDateTime read(JsonReader in) throws IOException {
            return LocalDateTime.parse(in.nextString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
    }

    static final class InstantTypeAdapter extends TypeAdapter<Instant> {

        @Override
        public void write(JsonWriter out, Instant value) throws IOException {
            out.value(DateTimeFormatter.ISO_INSTANT.format(value));
        }

        @Override
        public Instant read(JsonReader in) throws IOException {
            return DateTimeFormatter.ISO_INSTANT.parse(in.nextString(), Instant::from);
        }
    }

    static final class DurationTypeAdapter extends TypeAdapter<Duration> {

        @Override
        public void write(JsonWriter out, Duration value) throws IOException {
            out.value(value.get(ChronoUnit.SECONDS));
        }

        @Override
        public Duration read(JsonReader in) throws IOException {
            return Duration.of(in.nextLong(), ChronoUnit.SECONDS);
        }
    }

    static final class LocalDateTypeAdapter extends TypeAdapter<LocalDate> {

        @Override
        public void write(JsonWriter out, LocalDate value) throws IOException {
            out.value(DateTimeFormatter.ISO_LOCAL_DATE.format(value));
        }

        @Override
        public LocalDate read(JsonReader in) throws IOException {
            return DateTimeFormatter.ISO_LOCAL_DATE.parse(in.nextString(), LocalDate::from);
        }
    }

    static final class MonthDayTypeAdapter extends TypeAdapter<MonthDay> {

        private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M");

        @Override
        public void write(JsonWriter out, MonthDay value) throws IOException {
            out.value(value.format(formatter));
        }

        @Override
        public MonthDay read(JsonReader in) throws IOException {
            return MonthDay.parse(in.nextString(), formatter);
        }
    }

    static final class PeriodTypeAdapter extends TypeAdapter<Period> {

        @Override
        public void write(JsonWriter out, Period value) throws IOException {
            out.value(value.toString());
        }

        @Override
        public Period read(JsonReader in) throws IOException {
            return Period.parse(in.nextString());
        }
    }

    static final class YearTypeAdapter extends TypeAdapter<Year> {

        @Override
        public void write(JsonWriter out, Year value) throws IOException {
            out.value(value.getValue());
        }

        @Override
        public Year read(JsonReader in) throws IOException {
            return Year.of(in.nextInt());
        }
    }

    static final class YearMonthAdapter extends TypeAdapter<YearMonth> {

        @Override
        public void write(JsonWriter out, YearMonth value) throws IOException {
            out.value(value.toString());
        }

        @Override
        public YearMonth read(JsonReader in) throws IOException {
            return YearMonth.parse(in.nextString());
        }
    }

    static final class UtcDateTypeAdapter extends TypeAdapter<Date> {

        private final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

        @Override
        public void write(JsonWriter out, Date date) throws IOException {
            if (date == null) {
                out.nullValue();
            } else {
                String value = format(date, true, UTC_TIME_ZONE);
                out.value(value);
            }
        }

        @Override
        public Date read(JsonReader in) throws IOException {
            try {
                if (in.peek() == JsonToken.NULL) {
                    in.nextNull();
                    return null;
                }
                String date = in.nextString();
                // Instead of using iso8601Format.parse(value), we use Jackson's date parsing
                // This is because Android doesn't support XXX because it is JDK 1.6
                return parse(date, new ParsePosition(0));
            } catch (ParseException e) {
                throw new JsonParseException(e);
            }
        }

        // Date parsing code from Jackson databind ISO8601Utils.java
        // https://github.com/FasterXML/jackson-databind/blob/master/src/main/java/com/fasterxml/jackson/databind/util/ISO8601Utils.java
        private static final String GMT_ID = "GMT";

        /**
         * Format date into yyyy-MM-ddThh:mm:ss[.sss][Z|[+-]hh:mm]
         *
         * @param date   the date to format
         * @param millis true to include millis precision otherwise false
         * @param tz     timezone to use for the formatting (GMT will produce 'Z')
         * @return the date formatted as yyyy-MM-ddThh:mm:ss[.sss][Z|[+-]hh:mm]
         */
        private static String format(Date date, boolean millis, TimeZone tz) {
            Calendar calendar = new GregorianCalendar(tz, Locale.US);
            calendar.setTime(date);

            // estimate capacity of buffer as close as we can (yeah, that's pedantic ;)
            int capacity = "yyyy-MM-ddThh:mm:ss".length();
            capacity += millis ? ".sss".length() : 0;
            capacity += tz.getRawOffset() == 0 ? "Z".length() : "+hh:mm".length();
            StringBuilder formatted = new StringBuilder(capacity);

            padInt(formatted, calendar.get(Calendar.YEAR), "yyyy".length());
            formatted.append('-');
            padInt(formatted, calendar.get(Calendar.MONTH) + 1, "MM".length());
            formatted.append('-');
            padInt(formatted, calendar.get(Calendar.DAY_OF_MONTH), "dd".length());
            formatted.append('T');
            padInt(formatted, calendar.get(Calendar.HOUR_OF_DAY), "hh".length());
            formatted.append(':');
            padInt(formatted, calendar.get(Calendar.MINUTE), "mm".length());
            formatted.append(':');
            padInt(formatted, calendar.get(Calendar.SECOND), "ss".length());
            if (millis) {
                formatted.append('.');
                padInt(formatted, calendar.get(Calendar.MILLISECOND), "sss".length());
            }

            int offset = tz.getOffset(calendar.getTimeInMillis());
            if (offset != 0) {
                int hours = Math.abs((offset / (60 * 1000)) / 60);
                int minutes = Math.abs((offset / (60 * 1000)) % 60);
                formatted.append(offset < 0 ? '-' : '+');
                padInt(formatted, hours, "hh".length());
                formatted.append(':');
                padInt(formatted, minutes, "mm".length());
            } else {
                formatted.append('Z');
            }

            return formatted.toString();
        }

        /**
         * Zero pad a number to a specified length
         *
         * @param buffer buffer to use for padding
         * @param value  the integer value to pad if necessary.
         * @param length the length of the string we should zero pad
         */
        private static void padInt(StringBuilder buffer, int value, int length) {
            String strValue = Integer.toString(value);
            buffer.append("0".repeat(Math.max(0, length - strValue.length())));
            buffer.append(strValue);
        }

        /**
         * Parse a date from ISO-8601 formatted string. It expects a format
         * [yyyy-MM-dd|yyyyMMdd][T(hh:mm[:ss[.sss]]|hhmm[ss[.sss]])]?[Z|[+-]hh:mm]]
         *
         * @param date ISO string to parse in the appropriate format.
         * @param pos  The position to start parsing from, updated to where parsing stopped.
         * @return the parsed date
         * @throws ParseException if the date is not in the appropriate format
         */
        private static Date parse(String date, ParsePosition pos) throws ParseException {
            Exception fail;
            try {
                int offset = pos.getIndex();

                // extract year
                int year = parseInt(date, offset, offset += 4);
                if (checkOffset(date, offset, '-')) {
                    offset += 1;
                }

                // extract month
                int month = parseInt(date, offset, offset += 2);
                if (checkOffset(date, offset, '-')) {
                    offset += 1;
                }

                // extract day
                int day = parseInt(date, offset, offset += 2);
                // default time value
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                int milliseconds =
                        0; // always use 0 otherwise returned date will include millis of current time
                if (checkOffset(date, offset, 'T')) {

                    // extract hours, minutes, seconds and milliseconds
                    hour = parseInt(date, offset += 1, offset += 2);
                    if (checkOffset(date, offset, ':')) {
                        offset += 1;
                    }

                    minutes = parseInt(date, offset, offset += 2);
                    if (checkOffset(date, offset, ':')) {
                        offset += 1;
                    }
                    // second and milliseconds can be optional
                    if (date.length() > offset) {
                        char c = date.charAt(offset);
                        if (c != 'Z' && c != '+' && c != '-') {
                            seconds = parseInt(date, offset, offset += 2);
                            // milliseconds can be optional in the format
                            if (checkOffset(date, offset, '.')) {
                                milliseconds = parseInt(date, offset += 1, offset += 3);
                            }
                        }
                    }
                }

                // extract timezone
                String timezoneId;
                if (date.length() <= offset) {
                    throw new IllegalArgumentException("No time zone indicator");
                }
                char timezoneIndicator = date.charAt(offset);
                if (timezoneIndicator == '+' || timezoneIndicator == '-') {
                    String timezoneOffset = date.substring(offset);
                    timezoneId = GMT_ID + timezoneOffset;
                    offset += timezoneOffset.length();
                } else if (timezoneIndicator == 'Z') {
                    timezoneId = GMT_ID;
                    offset += 1;
                } else {
                    throw new IndexOutOfBoundsException("Invalid time zone indicator " + timezoneIndicator);
                }

                TimeZone timezone = TimeZone.getTimeZone(timezoneId);
                if (!timezone.getID().equals(timezoneId)) {
                    throw new IndexOutOfBoundsException();
                }

                Calendar calendar = new GregorianCalendar(timezone);
                calendar.setLenient(false);
                calendar.set(Calendar.YEAR, year);
                calendar.set(Calendar.MONTH, month - 1);
                calendar.set(Calendar.DAY_OF_MONTH, day);
                calendar.set(Calendar.HOUR_OF_DAY, hour);
                calendar.set(Calendar.MINUTE, minutes);
                calendar.set(Calendar.SECOND, seconds);
                calendar.set(Calendar.MILLISECOND, milliseconds);

                pos.setIndex(offset);
                return calendar.getTime();
                // If we get a ParseException it'll already have the right message/offset.
                // Other exception types can convert here.
            } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                fail = e;
            }
            String input = (date == null) ? null : ("'" + date + "'");
            throw new ParseException(
                    "Failed to parse date [" + input + "]: " + fail.getMessage(), pos.getIndex());
        }

        /**
         * Check if the expected character exist at the given offset in the value.
         *
         * @param value    the string to check at the specified offset
         * @param offset   the offset to look for the expected character
         * @param expected the expected character
         * @return true if the expected character exist at the given offset
         */
        private static boolean checkOffset(String value, int offset, char expected) {
            return (offset < value.length()) && (value.charAt(offset) == expected);
        }

        /**
         * Parse an integer located between 2 given offsets in a string
         *
         * @param value      the string to parse
         * @param beginIndex the start index for the integer in the string
         * @param endIndex   the end index for the integer in the string
         * @return the int
         * @throws NumberFormatException if the value is not a number
         */
        private static int parseInt(String value, int beginIndex, int endIndex)
                throws NumberFormatException {
            if (beginIndex < 0 || endIndex > value.length() || beginIndex > endIndex) {
                throw new NumberFormatException(value);
            }
            // use same logic as in Integer.parseInt() but less generic we're not supporting negative
            // values
            int i = beginIndex;
            int result = 0;
            int digit;
            if (i < endIndex) {
                digit = Character.digit(value.charAt(i++), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid number: " + value);
                }
                result = -digit;
            }
            while (i < endIndex) {
                digit = Character.digit(value.charAt(i++), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid number: " + value);
                }
                result *= 10;
                result -= digit;
            }
            return -result;
        }
    }
}
