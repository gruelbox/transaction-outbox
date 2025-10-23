package com.gruelbox.transactionoutbox.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

class TransactionOutboxEntryDeserializer extends JsonDeserializer<TransactionOutboxEntry> {

  @Override
  public TransactionOutboxEntry deserialize(JsonParser p, DeserializationContext c)
      throws IOException {
    ObjectCodec oc = p.getCodec();
    JsonNode node = oc.readTree(p);
    var i = node.get("invocation");
    var mdc = i.get("mdc");
    var session = i.get("session");
    return TransactionOutboxEntry.builder()
        .id(node.get("id").asText())
        .lastAttemptTime(mapJsonInstant(node, "lastAttemptTime", c))
        .nextAttemptTime(mapJsonInstant(node, "nextAttemptTime", c))
        .attempts(node.get("attempts").asInt())
        .blocked(node.get("blocked").asBoolean())
        .processed(node.get("processed").asBoolean())
        .uniqueRequestId(mapJsonNull(node.get("uniqueRequestId"), JsonNode::asText))
        .version(node.get("version").asInt())
        .invocation(
            new Invocation(
                i.get("className").asText(),
                i.get("methodName").asText(),
                c.readTreeAsValue(i.get("parameterTypes"), Class[].class),
                c.readTreeAsValue(i.get("args"), Object[].class),
                mdc.isNull()
                    ? null
                    : c.readTreeAsValue(
                        mdc,
                        c.getTypeFactory()
                            .constructType(new TypeReference<Map<String, String>>() {})),
                session == null || session.isNull()
                    ? null
                    : c.readTreeAsValue(
                        session,
                        c.getTypeFactory()
                            .constructType(new TypeReference<Map<String, String>>() {}))))
        .build();
  }

  private Instant mapJsonInstant(JsonNode node, String nextAttemptTime, DeserializationContext c)
      throws IOException {
    return mapJsonNull(node.get(nextAttemptTime), n -> c.readTreeAsValue(n, Instant.class));
  }

  private <T> T mapJsonNull(JsonNode jsonNode, JsonThrowingFunction<JsonNode, T> fn)
      throws IOException {
    if (jsonNode == null) {
      return null;
    }
    if (jsonNode.isNull()) {
      return null;
    }
    return fn.apply(jsonNode);
  }

  @FunctionalInterface
  private interface JsonThrowingFunction<T, U> {
    U apply(T t) throws IOException;
  }
}
