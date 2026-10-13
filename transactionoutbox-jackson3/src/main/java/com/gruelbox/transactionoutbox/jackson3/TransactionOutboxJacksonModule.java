package com.gruelbox.transactionoutbox.jackson3;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import tools.jackson.core.Version;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.TypeResolverBuilder;
import tools.jackson.databind.jsontype.impl.DefaultTypeResolverBuilder;
import tools.jackson.databind.module.SimpleDeserializers;
import tools.jackson.databind.module.SimpleSerializers;

public class TransactionOutboxJacksonModule extends JacksonModule {

  @Override
  public String getModuleName() {
    return "TransactionOutboxJacksonModule";
  }

  @Override
  public Version version() {
    return Version.unknownVersion();
  }

  @Override
  public void setupModule(final SetupContext context) {
    final SimpleSerializers serializers = new SimpleSerializers();
    serializers.addSerializer(Invocation.class, new CustomInvocationSerializer());

    final SimpleDeserializers deserializers = new SimpleDeserializers();
    deserializers.addDeserializer(Invocation.class, new CustomInvocationDeserializer());
    deserializers.addDeserializer(
        TransactionOutboxEntry.class, new TransactionOutboxEntryDeserializer());

    context.addSerializers(serializers);
    context.addDeserializers(deserializers);
  }

  public static TypeResolverBuilder<?> typeResolver() {
    return new DefaultTypeResolverBuilder(
        BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build(),
        DefaultTyping.NON_FINAL,
        JsonTypeInfo.As.WRAPPER_OBJECT);
  }
}
