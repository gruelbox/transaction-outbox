package com.gruelbox.transactionoutbox.jackson3;

import com.gruelbox.transactionoutbox.Invocation;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ClassUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

class CustomInvocationDeserializer extends StdDeserializer<Invocation> {

  private static final Pattern IMMUTABLE_COLLECTION =
      Pattern.compile("java\\.util\\.ImmutableCollections\\$(Set|Map|List)[\\dN]+");

  CustomInvocationDeserializer() {
    super(Invocation.class);
  }

  @Override
  public Invocation deserializeWithType(
      final JsonParser p, final DeserializationContext ctx, final TypeDeserializer typeDeserializer)
      throws JacksonException {
    return deserialize(p, ctx);
  }

  @Override
  public Invocation deserialize(final JsonParser p, final DeserializationContext ctx)
      throws JacksonException {
    final JsonNode node = ctx.readTree(p);
    final String className = node.get("className").stringValue();
    final String methodName = node.get("methodName").stringValue();
    final JsonNode paramTypes = node.get("parameterTypes");
    final JsonNode arguments = node.get("args");
    final JsonNode processedArguments = replaceImmutableCollections(arguments);
    final Class<?>[] types = new Class<?>[paramTypes.size()];

    for (int i = 0; i < paramTypes.size(); i++) {
      try {
        types[i] = ClassUtils.getClass(paramTypes.get(i).asString());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    final Object[] args = ctx.readTreeAsValue(processedArguments, Object[].class);

    final Map<String, String> mdc = readStringMap(node.get("mdc"), ctx);
    final Map<String, String> session = readStringMap(node.get("session"), ctx);

    return new Invocation(className, methodName, types, args, mdc, session);
  }

  private JsonNode replaceImmutableCollections(final JsonNode node) {
    if (node instanceof ObjectNode objectNode) {
      if (objectNode.size() == 1) {
        final String name = objectNode.propertyNames().iterator().next();
        final Matcher matcher = IMMUTABLE_COLLECTION.matcher(name);
        if (matcher.matches()) {
          final String replacement =
              switch (matcher.group(1)) {
                case "Set" -> "java.util.HashSet";
                case "Map" -> "java.util.HashMap";
                default -> "java.util.ArrayList";
              };
          objectNode.set(replacement, objectNode.remove(name));
        }
      }
      objectNode.properties().forEach(e -> e.setValue(replaceImmutableCollections(e.getValue())));
    } else if (node instanceof ArrayNode arr) {
      for (int i = 0; i < arr.size(); i++) {
        arr.set(i, replaceImmutableCollections(arr.get(i)));
      }
    }
    return node;
  }

  private Map<String, String> readStringMap(final JsonNode node, final DeserializationContext ctx) {
    if (node == null || node.isNull()) {
      return null;
    }
    return ctx.readTreeAsValue(
        node, ctx.getTypeFactory().constructMapType(Map.class, String.class, String.class));
  }
}
