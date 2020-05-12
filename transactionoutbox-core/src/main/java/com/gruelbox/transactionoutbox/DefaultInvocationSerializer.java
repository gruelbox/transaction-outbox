package com.gruelbox.transactionoutbox;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DefaultInvocationSerializer implements InvocationSerializer {

  private final Gson gson;

  DefaultInvocationSerializer() {
    this.gson =
        new GsonBuilder()
            .registerTypeAdapter(Invocation.class, new InvocationJsonSerializer())
            .registerTypeAdapter(Date.class, new UtcDateTypeAdapter())
            .excludeFieldsWithModifiers(Modifier.TRANSIENT, Modifier.STATIC)
            .create();
  }

  @Override
  public void serializeInvocation(Invocation invocation, Writer writer) {
    gson.toJson(invocation, writer);
  }

  @Override
  public Invocation deserializeInvocation(Reader reader) {
    return gson.fromJson(reader, Invocation.class);
  }

  private static final class InvocationJsonSerializer
      implements JsonSerializer<Invocation>, JsonDeserializer<Invocation> {

    private Map<Class<?>, String> classToName = new HashMap<>();
    private Map<String, Class<?>> nameToClass = new HashMap<>();

    InvocationJsonSerializer() {
      addClassPair(byte.class, "byte");
      addClassPair(short.class, "short");
      addClassPair(int.class, "int");
      addClassPair(long.class, "long");
      addClassPair(float.class, "float");
      addClassPair(double.class, "double");
      addClassPair(boolean.class, "boolean");
      addClassPair(char.class, "char");

      addClassPair(Byte.class, "Byte");
      addClassPair(Short.class, "Short");
      addClassPair(Integer.class, "Integer");
      addClassPair(Long.class, "Long");
      addClassPair(Float.class, "Float");
      addClassPair(Float.class, "Float");
      addClassPair(Float.class, "Float");
      addClassPair(Double.class, "Double");
      addClassPair(Boolean.class, "Boolean");
      addClassPair(Character.class, "Character");

      addClassPair(BigDecimal.class, "BigDecimal");
      addClassPair(String.class, "String");
      addClassPair(Date.class, "Date");

      addClassPair(DayOfWeek.class, "DayOfWeek");
      addClassPair(Duration.class, "Duration");
      addClassPair(Instant.class, "Instant");
      addClassPair(LocalDate.class, "LocalDate");
      addClassPair(LocalDateTime.class, "LocalDateTime");
      addClassPair(Month.class, "Month");
      addClassPair(MonthDay.class, "MonthDay");
      addClassPair(Period.class, "Period");
      addClassPair(Year.class, "Year");
      addClassPair(YearMonth.class, "YearMonth");
      addClassPair(ZoneOffset.class, "ZoneOffset");
      addClassPair(DayOfWeek.class, "DayOfWeek");
      addClassPair(ChronoUnit.class, "ChronoUnit");
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
        return classLoader != null ? classLoader.loadClass(name) : Class.forName(name);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public JsonElement serialize(Invocation src, Type typeOfSrc, JsonSerializationContext context) {
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
      Object[] args = new Object[jsonParams.size()];
      for (int i = 0; i < jsonParams.size(); i++) {
        JsonElement param = jsonParams.get(i);
        Class<?> paramClass = classForName(param.getAsJsonObject().get("t").getAsString());
        params[i] = paramClass;
        JsonElement paramValue = param.getAsJsonObject().get("v");
        try {
          args[i] = context.deserialize(paramValue, paramClass);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to deserialize parameter [" + paramValue + "] of type [" + paramClass + "]",
              e);
        }
      }

      return new Invocation(className, methodName, params, args);
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

  public static final class UtcDateTypeAdapter extends TypeAdapter<Date> {
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
     * @param date the date to format
     * @param millis true to include millis precision otherwise false
     * @param tz timezone to use for the formatting (GMT will produce 'Z')
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
     * @param value the integer value to pad if necessary.
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
     * @param pos The position to start parsing from, updated to where parsing stopped.
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
     * @param value the string to check at the specified offset
     * @param offset the offset to look for the expected character
     * @param expected the expected character
     * @return true if the expected character exist at the given offset
     */
    private static boolean checkOffset(String value, int offset, char expected) {
      return (offset < value.length()) && (value.charAt(offset) == expected);
    }

    /**
     * Parse an integer located between 2 given offsets in a string
     *
     * @param value the string to parse
     * @param beginIndex the start index for the integer in the string
     * @param endIndex the end index for the integer in the string
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
