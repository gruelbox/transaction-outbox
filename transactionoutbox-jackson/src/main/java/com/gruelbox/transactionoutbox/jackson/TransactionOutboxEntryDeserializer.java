package com.gruelbox.transactionoutbox.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

class TransactionOutboxEntryDeserializer extends JsonDeserializer<TransactionOutboxEntry> {

  @Override
  public TransactionOutboxEntry deserialize(JsonParser p, DeserializationContext c)
      throws IOException {
    ObjectCodec oc = p.getCodec();
    JsonNode entry = oc.readTree(p);
    var invocation = entry.get("invocation");
    return TransactionOutboxEntry.builder()
        .id(entry.get("id").asText())
        .lastAttemptTime(mapNullableInstant(entry.get("lastAttemptTime"), c))
        .nextAttemptTime(mapNullableInstant(entry.get("nextAttemptTime"), c))
        .attempts(entry.get("attempts").asInt())
        .blocked(entry.get("blocked").asBoolean())
        .processed(entry.get("processed").asBoolean())
        .uniqueRequestId(mapNullableString(entry.get("uniqueRequestId")))
        .version(entry.get("version").asInt())
        .invocation(
            new Invocation(
                invocation.get("className").asText(),
                invocation.get("methodName").asText(),
                c.readTreeAsValue(invocation.get("parameterTypes"), Class[].class),
                c.readTreeAsValue(invocation.get("args"), Object[].class),
                mapNullableStringMap(invocation.get("mdc"), c),
                mapNullableStringMap(invocation.get("session"), c)))
        .build();
  }

  private String mapNullableString(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  private Instant mapNullableInstant(JsonNode node, DeserializationContext c) throws IOException {
    if (node == null || node.isNull()) {
      return null;
    }
    return c.readTreeAsValue(node, Instant.class);
  }

  private Map<String, String> mapNullableStringMap(JsonNode node, DeserializationContext c) {
    if (node == null || node.isNull()) {
      return null;
    }
    Map<String, String> result = new HashMap<>();
    node.forEachEntry((key, value) -> result.put(key, value.asText()));
    return result;
  }
}
