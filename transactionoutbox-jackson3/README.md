# transaction-outbox-jackson3

[![Jackson 3 on Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-jackson3/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-jackson3)
[![Jackson 3 Javadoc](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-jackson3.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-jackson3)

Extension for [transaction-outbox-core](../README.md) which uses [Jackson 3](https://github.com/FasterXML/jackson) for
serialisation.
It is the Jackson 3 counterpart of [transaction-outbox-jackson](../transactionoutbox-jackson/README.md) and uses the
same stored format.

However, if there is any risk that you might not trust the source of the serialized `Invocation`,
_do not use this_. This serializer is vulnerable to
[deserialization of untrusted data](https://github.com/gruelbox/transaction-outbox/issues/236#issuecomment-1024929436),
which is why it is not included in the core library.

## Installation

#### Maven

```xml

<dependency>
    <groupId>com.gruelbox</groupId>
    <artifactId>transactionoutbox-jackson3</artifactId>
    <version>7.1.769</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-jackson3:7.1.769'
```

## Configuration

### Fresh projects

```java
var outbox = TransactionOutbox.builder()
        .persistor(DefaultPersistor.builder()
                .dialect(Dialect.H2)
                .serializer(JacksonInvocationSerializer.builder()
                        .mapper(JsonMapper.builder().build())
                        .build())
                .build());
```

### Existing projects using DefaultInvocationSerializer

To keep loading tasks queued in the older GSON format, pass a `DefaultInvocationSerializer` matching your previous
configuration:

```java
.serializer(JacksonInvocationSerializer.builder()
  .mapper(JsonMapper.builder().build())
        .defaultInvocationSerializer(DefaultInvocationSerializer.builder()
    .serializableTypes(Set.of(Foo.class, Bar.class))
        .build())
        .build())
```

## Migrating from transactionoutbox-jackson

1. Replace the `transactionoutbox-jackson` dependency with `transactionoutbox-jackson3`.
2. Change imports from `com.gruelbox.transactionoutbox.jackson` to `com.gruelbox.transactionoutbox.jackson3`,
   and build your mapper with `tools.jackson.databind.json.JsonMapper`.
3. Remove `JavaTimeModule` / `Jdk8Module` registrations; Jackson 3 supports these types natively.

Tasks already in the database remain readable: both modules use the same stored format, and the
Jackson 3 serializer pins the settings whose defaults changed in Jackson 3 (enum and date/time handling).
Mixed deployments are safe in both directions, so you can do a rolling deploy or roll back to a version
using `transactionoutbox-jackson` while tasks are queued.

**Caveat:** Jackson 3 ignores Jackson 2 *databind* annotations such as `@JsonSerialize`, `@JsonDeserialize`
or `@JsonNaming` (`com.fasterxml.jackson.databind.annotation`). If your task arguments rely on these, switch
them to their `tools.jackson.databind.annotation` equivalents before migrating, otherwise those arguments
will be serialized differently. Core annotations such as `@JsonProperty`, `@JsonCreator` and `@JsonTypeInfo`
are unaffected.

## Usage

You can now go wild with your scheduled method arguments:

```java
outbox.schedule(getClass())
        .process(List.of(LocalDate.of(2000,1,1), "a", "b", 2));
```

## Serializing TransactionOutboxEntry

```java
ObjectMapper objectMapper = yourNormalSharedObjectMapper.rebuild()
        .addModule(new TransactionOutboxJacksonModule())
        .setDefaultTyping(TransactionOutboxJacksonModule.typeResolver())
        .build();

String message = objectMapper.writeValueAsString(entry);
TransactionOutboxEntry entry = objectMapper.readValue(message, TransactionOutboxEntry.class);
```