package com.gruelbox.transactionoutbox.jackson3;

import com.gruelbox.transactionoutbox.Invocation;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.ser.std.StdSerializer;

class CustomInvocationSerializer extends StdSerializer<Invocation> {

  public CustomInvocationSerializer() {
    this(Invocation.class);
  }

  protected CustomInvocationSerializer(Class<Invocation> t) {
    super(t);
  }

  @Override
  public void serialize(
      final Invocation value, final JsonGenerator gen, final SerializationContext ctx)
      throws JacksonException {
    gen.writeStartObject();
    gen.writeStringProperty("className", value.getClassName());
    gen.writeStringProperty("methodName", value.getMethodName());
    gen.writeArrayPropertyStart("parameterTypes");
    for (Class<?> parameterType : value.getParameterTypes()) {
      gen.writeString(parameterType.getCanonicalName());
    }
    gen.writeEndArray();
    gen.writePOJOProperty("args", value.getArgs());
    gen.writePOJOProperty("mdc", value.getMdc());
    gen.writePOJOProperty("session", value.getSession());
    gen.writeEndObject();
  }

  @Override
  public void serializeWithType(
      final Invocation value,
      final JsonGenerator gen,
      final SerializationContext ctx,
      final TypeSerializer typeSer)
      throws JacksonException {
    serialize(value, gen, ctx);
  }
}
