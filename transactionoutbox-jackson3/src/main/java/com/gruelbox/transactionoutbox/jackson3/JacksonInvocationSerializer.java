package com.gruelbox.transactionoutbox.jackson3;

import com.gruelbox.transactionoutbox.DefaultInvocationSerializer;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import lombok.Builder;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.cfg.EnumFeature;

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
public final class JacksonInvocationSerializer implements InvocationSerializer {
  private final ObjectMapper mapper;
  private final InvocationSerializer defaultInvocationSerializer;

  @Builder
  private JacksonInvocationSerializer(
      final ObjectMapper mapper, final DefaultInvocationSerializer defaultInvocationSerializer) {
    this.mapper =
        mapper
            .rebuild()
            .setDefaultTyping(TransactionOutboxJacksonModule.typeResolver())
            .addModule(new TransactionOutboxJacksonModule())
            .disable(EnumFeature.WRITE_ENUMS_USING_TO_STRING)
            .disable(EnumFeature.READ_ENUMS_USING_TO_STRING)
            .enable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
            .build();
    this.defaultInvocationSerializer = defaultInvocationSerializer;
  }

  @Override
  public void serializeInvocation(final Invocation invocation, final Writer writer) {
    mapper.writeValue(writer, invocation);
  }

  @Override
  public Invocation deserializeInvocation(final Reader reader) throws IOException {
    final BufferedReader br = new BufferedReader(reader);
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
  }

  private boolean checkForOldSerialization(final BufferedReader reader) throws IOException {
    reader.mark(6);
    final char[] chars = new char[6];
    final int charsRead = reader.read(chars, 0, 6);

    String result = "";
    if (charsRead != -1) {
      result = new String(chars, 0, charsRead);
    }
    reader.reset();
    return result.startsWith("{\"c\":");
  }
}
