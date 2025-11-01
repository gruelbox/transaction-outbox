package com.gruelbox.transactionoutbox.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.gruelbox.transactionoutbox.Invocation;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Map;

class CustomInvocationSerializer extends StdSerializer<Invocation> {

  public CustomInvocationSerializer() {
    this(Invocation.class);
  }

  protected CustomInvocationSerializer(Class<Invocation> t) {
    super(t);
  }

  @Override
  public void serializeWithType(
      Invocation value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
      throws IOException {
    serialize(value, gen, serializers);
  }

  @Override
  public void serialize(Invocation value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeStartObject();
    gen.writeStringField("className", value.getClassName());
    gen.writeStringField("methodName", value.getMethodName());
    gen.writeArrayFieldStart("parameterTypes");
    for (Class<?> parameterType : value.getParameterTypes()) {
      gen.writeString(parameterType.getCanonicalName());
    }
    gen.writeEndArray();
    gen.writeObjectField("args", value.getArgs());

    writeMap("mdc", value.getMdc(), gen);
    writeMap("session", value.getSession(), gen);

    gen.writeEndObject();
  }

  private static void writeMap(String field, Map<String, String> map, JsonGenerator gen)
      throws IOException {
    if (map != null && isFinal(map.getClass())) {
      gen.writeObjectFieldStart(field);
      gen.writeObjectField(Map.class.getName(), map);
      gen.writeEndObject();
    } else {
      gen.writeObjectField(field, map);
    }
  }

  private static boolean isFinal(Class<?> clazz) {
    return Modifier.isFinal(clazz.getModifiers());
  }
}
