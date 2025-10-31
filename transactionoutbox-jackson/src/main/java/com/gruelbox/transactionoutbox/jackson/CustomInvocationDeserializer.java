package com.gruelbox.transactionoutbox.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.gruelbox.transactionoutbox.Invocation;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;

@Slf4j
class CustomInvocationDeserializer extends StdDeserializer<Invocation> {

  private static final Pattern setPattern =
      Pattern.compile("\\{\\w*\"(java.util.ImmutableCollections\\$Set[\\dN]+)\"\\w*:");
  private static final Pattern mapPattern =
      Pattern.compile("\\{\\w*\"(java.util.ImmutableCollections\\$Map[\\dN]+)\"\\w*:");
  private static final Pattern listPattern =
      Pattern.compile("\\{\\w*\"(java.util.ImmutableCollections\\$List[\\dN]+)\"\\w*:");

  protected CustomInvocationDeserializer(Class<?> vc) {
    super(vc);
  }

  CustomInvocationDeserializer() {
    this(Invocation.class);
  }

  @Override
  public Invocation deserializeWithType(
      JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
      throws IOException {
    return deserialize(p, ctxt);
  }

  @Override
  public Invocation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    String className = node.get("className").textValue();
    String methodName = node.get("methodName").textValue();
    ArrayNode paramTypes = ((ArrayNode) node.get("parameterTypes"));
    JsonNode arguments = node.get("args");
    JsonNode processedArguments = replaceImmutableCollections(arguments, p);
    Class<?>[] types = new Class<?>[paramTypes.size()];

    for (int i = 0; i < paramTypes.size(); i++) {
      try {
        types[i] = ClassUtils.getClass(paramTypes.get(i).asText());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    Object[] args = p.getCodec().treeToValue(processedArguments, Object[].class);

    Map<String, String> mdc =
        p.getCodec()
            .readValue(p.getCodec().treeAsTokens(node.get("mdc")), new TypeReference<>() {});

    var sessionNode = node.get("session");
    Map<String, String> session = null;
    if (sessionNode != null && !sessionNode.isNull()) {
      Map<String, String> sessTmp =
          p.getCodec().readValue(p.getCodec().treeAsTokens(sessionNode), new TypeReference<>() {});
      session = new HashMap<>(sessTmp);
    }
    return new Invocation(className, methodName, types, args, mdc, session);
  }

  private JsonNode replaceImmutableCollections(JsonNode arguments, JsonParser p)
      throws IOException {
    String args = arguments.toString();
    args = setPattern.matcher(args).replaceAll("{\"java.util.HashSet\":");
    args = mapPattern.matcher(args).replaceAll("{\"java.util.HashMap\":");
    args = listPattern.matcher(args).replaceAll("{\"java.util.ArrayList\":");
    JsonParser parser = p.getCodec().getFactory().createParser(args);
    return p.getCodec().readTree(parser);
  }
}
