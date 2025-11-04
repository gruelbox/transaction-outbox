package com.gruelbox.transactionoutbox.jackson;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;

public class TransactionOutboxJacksonModule extends Module {

  @Override
  public String getModuleName() {
    return "TransactionOutboxJacksonModule";
  }

  @Override
  public Version version() {
    return Version.unknownVersion();
  }

  @Override
  public void setupModule(SetupContext setupContext) {
    SimpleSerializers serializers = new SimpleSerializers();
    serializers.addSerializer(Invocation.class, new CustomInvocationSerializer());
    setupContext.addSerializers(serializers);

    SimpleDeserializers deserializers = new SimpleDeserializers();
    deserializers.addDeserializer(Invocation.class, new CustomInvocationDeserializer());
    deserializers.addDeserializer(
        TransactionOutboxEntry.class, new TransactionOutboxEntryDeserializer());
    setupContext.addDeserializers(deserializers);
  }

  public static TypeResolverBuilder<?> typeResolver() {
    return new ObjectMapper.DefaultTypeResolverBuilder(
            JAVA_LANG_OBJECT,
            BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build())
        .init(JsonTypeInfo.Id.CLASS, null)
        .inclusion(JsonTypeInfo.As.WRAPPER_OBJECT);
  }
}
