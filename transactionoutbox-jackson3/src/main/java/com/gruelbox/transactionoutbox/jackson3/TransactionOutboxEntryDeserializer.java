package com.gruelbox.transactionoutbox.jackson3;

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.time.Instant;
import java.util.Map;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

class TransactionOutboxEntryDeserializer extends ValueDeserializer<TransactionOutboxEntry> {

  @Override
  public TransactionOutboxEntry deserialize(final JsonParser p, final DeserializationContext c)
      throws JacksonException {
    final JsonNode entry = c.readTree(p);
    final JsonNode invocation = entry.get("invocation");
    return TransactionOutboxEntry.builder()
        .id(entry.get("id").asString())
        .lastAttemptTime(mapNullableInstant(entry.get("lastAttemptTime"), c))
        .nextAttemptTime(mapNullableInstant(entry.get("nextAttemptTime"), c))
        .attempts(entry.get("attempts").asInt())
        .blocked(entry.get("blocked").asBoolean())
        .processed(entry.get("processed").asBoolean())
        .uniqueRequestId(mapNullableString(entry.get("uniqueRequestId")))
        .version(entry.get("version").asInt())
        .invocation(
            new Invocation(
                invocation.get("className").asString(),
                invocation.get("methodName").asString(),
                c.readTreeAsValue(invocation.get("parameterTypes"), Class[].class),
                c.readTreeAsValue(invocation.get("args"), Object[].class),
                mapNullableStringMap(invocation.get("mdc"), c),
                mapNullableStringMap(invocation.get("session"), c)))
        .build();
  }

  private String mapNullableString(final JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asString();
  }

  private Instant mapNullableInstant(final JsonNode node, final DeserializationContext c) {
    if (node == null || node.isNull()) {
      return null;
    }
    return c.readTreeAsValue(node, Instant.class);
  }

  private Map<String, String> mapNullableStringMap(
      final JsonNode node, final DeserializationContext c) {
    if (node == null || node.isNull()) {
      return null;
    }
    return c.readTreeAsValue(
        node, c.getTypeFactory().constructMapType(Map.class, String.class, String.class));
  }
}
