# transaction-outbox-jackson

[![Jackson on Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-jackson/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-guice)
[![Jackson Javadoc](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-jackson.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-guice)
[![Latest snapshot](https://img.shields.io/github/v/tag/gruelbox/transaction-outbox?label=snapshot&sort=semver)](#development-snapshots)

Extension for [transaction-outbox-core](../README.md) which uses Jackson for serialisation.

If you are confident in trusting your database, then this serializer has a number of advantages: it is as
configurable as whatever Jackson's `ObjectMapper` can handle, and explicitly handles n-depth polymorphic trees. This
largely means that you can throw pretty much anything at it and it will "just work".

However, if there is any risk that you might not trust the source of the serialized `Invocation`,
_do not use this_. This serializer is vulnerable to
[deserialization of untrusted data](https://github.com/gruelbox/transaction-outbox/issues/236#issuecomment-1024929436),
which is why it is not included in the core library.

## Installation

### Stable releases

#### Maven

```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-jackson</artifactId>
  <version>6.0.609</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-jackson:6.0.609'
```

### Development snapshots

See [transactionoutbox-core](../README.md) for more information.

## Configuration

### Fresh projects

If starting with a fresh project, you don't need to worry about compatibility with DefaultInvocationSerializer, so configure as follows:

```java
var outbox = TransactionOutbox.builder()
  .persistor(DefaultPersistor.builder()
    .dialect(Dialect.H2)
    .serializer(JacksonInvocationSerializer.builder()
        .mapper(new ObjectMapper())
        .build())
    .build())
```

### Existing projects using DefaultInvocationSerializer

If you're already using Transaction Outbox, you may have outbox tasks queued which your application needs to continue to be capable of loading.
To handle this, pass through an instance of `DefaultInvocationSerializer` which matches what you used previously:

```java
var outbox = TransactionOutbox.builder()
  .persistor(DefaultPersistor.builder()
    .dialect(Dialect.H2)
    .serializer(JacksonInvocationSerializer.builder()
      .mapper(new ObjectMapper())
      .defaultInvocationSerializer(DefaultInvocationSerializer.builder()
        .serializableTypes(Set.of(Foo.class, Bar.class))
        .build())
      .build())
    .build())
```

## Usage

You can now go wild with your scheduled method arguments:

```java
outbox.schedule(getClass())
  .process(List.of(LocalDate.of(2000,1,1), "a", "b", 2));
```
