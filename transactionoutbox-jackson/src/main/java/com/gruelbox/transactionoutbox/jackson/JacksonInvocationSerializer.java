package com.gruelbox.transactionoutbox.jackson;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.NON_FINAL;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.gruelbox.transactionoutbox.DefaultInvocationSerializer;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import lombok.Builder;

/**
 * A general-purpose {@link InvocationSerializer} which can handle pretty much anything that you
 * throw at it.
 *
 * <p>Note that if there is any risk that you might not trust the source of the serialized {@link
 * Invocation}, <strong>do not use this</strong>. This serializer is vulnerable to a
 * <em>deserialization of untrusted data</em> vulnerability (more information <a
 * href="https://github.com/gruelbox/transaction-outbox/issues/236#issuecomment-1024929436">here</a>)
 * which is why it is not included in the core library.
 */
public class JacksonInvocationSerializer implements InvocationSerializer {
  private final ObjectMapper mapper;
  private final InvocationSerializer defaultInvocationSerializer;

  @Builder
  public JacksonInvocationSerializer(
      ObjectMapper mapper, DefaultInvocationSerializer defaultInvocationSerializer) {
    this.mapper = mapper.copy();
    this.defaultInvocationSerializer = defaultInvocationSerializer;
    SimpleModule module = new SimpleModule();
    TypeResolverBuilder<?> typeResolver =
        new ObjectMapper.DefaultTypeResolverBuilder(
            NON_FINAL,
            BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build());
    typeResolver = typeResolver.init(JsonTypeInfo.Id.CLASS, null);
    typeResolver = typeResolver.inclusion(JsonTypeInfo.As.WRAPPER_OBJECT);
    this.mapper.setDefaultTyping(typeResolver);
    module.addSerializer(Invocation.class, new CustomInvocationSerializer());
    module.addDeserializer(Invocation.class, new CustomInvocationDeserializer(this.mapper));
    this.mapper.registerModule(module);
  }

  @Override
  public void serializeInvocation(Invocation invocation, Writer writer) {
    try {
      mapper.writeValue(writer, invocation);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Invocation deserializeInvocation(Reader reader) {
    try {
      // read ahead to check if old style
      BufferedReader br = new BufferedReader(reader);
      if (checkForOldSerialization(br)) {
        if (defaultInvocationSerializer == null) {
          throw new UnsupportedOperationException(
              "Can't deserialize GSON-format tasks without a "
                  + DefaultInvocationSerializer.class.getSimpleName()
                  + ". Supply one when building "
                  + getClass().getSimpleName());
        }
        return defaultInvocationSerializer.deserializeInvocation(br);
      }
      return mapper.readValue(br, Invocation.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean checkForOldSerialization(BufferedReader reader) throws IOException {
    reader.mark(1);
    char[] chars = new char[6];
    int charsRead = reader.read(chars, 0, 6);

    String result = "";
    if (charsRead != -1) {
      result = new String(chars, 0, charsRead);
    }
    reader.reset();
    return result.startsWith("{\"c\":");
  }
}
