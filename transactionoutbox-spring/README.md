# transaction-outbox-spring

[![Spring on Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-spring/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-spring)
[![Spring Javadoc](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-spring.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-spring)
[![Latest snapshot](https://img.shields.io/github/v/tag/gruelbox/transaction-outbox?label=snapshot&sort=semver)](#development-snapshots)

Extension for [transaction-outbox-core](../README.md) which integrates Spring's DI and/or transaction management.

I don't actually use Spring in production, so this is more presented as an example at the moment. Doubtless I've missed a lot of nuances about the flexibility of Spring. Pull requests very welcome.

## Installation

### Stable releases

The latest stable release is available from Maven Central.

#### Maven

```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-spring</artifactId>
  <version>6.0.609</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-spring:6.0.609'
```

### Development snapshots

Maven Central is updated regularly. Alternatively, if you want to stay at the bleeding edge, you can use continuously-delivered releases from [Github Package Repository](https://github.com/gruelbox/transaction-outbox/packages). These can be used from production builds since they will never be deleted.

#### Maven

```xml
<repositories>
  <repository>
    <id>github-transaction-outbox</id>
    <name>Gruelbox Github Repository</name>
    <url>https://maven.pkg.github.com/gruelbox/transaction-outbox</url>
  </repository>
</repositories>
```

You will need to authenticate with Github to use Github Package Repository. Create a personal access token in [your GitHub settings](https://github.com/settings/tokens). It only needs **read:package** permissions. Then add something like the following in your Maven `settings.xml`:

```xml
<servers>
    <server>
        <id>github-transaction-outbox</id>
        <username>${env.GITHUB_USERNAME}</username>
        <password>${env.GITHUB_TOKEN}</password>
    </server>
</servers>
```

The above example uses environment variables, allowing you to keep the credentials out of source control, but you can hard-code them if you know what you're doing.

## Example
An example application can be found here: https://github.com/gruelbox/transaction-outbox/tree/better-spring-example/transactionoutbox-spring/src/test.

## Configuration

Create your `TransactionOutbox` as a bean:

```java
@Bean
@Lazy
public TransactionOutbox transactionOutbox(SpringTransactionManager springTransactionManager,
                                           SpringInstantiator springInstantiator) {
  return TransactionOutbox.builder()
      .instantiator(springInstantiator)
      .transactionManager(springTransactionManager)
      .persistor(Persistor.forDialect(Dialect.H2))
      .build();

```

You can mix-and-match `SpringInstantiator` ans `SpringTransactionManager` with other implementations in hybrid frameworks.

## Usage

```java
@Transactional
public void doStuff() {
  customerRepository.save(new Customer(1L, "Martin", "Carthy"));
  customerRepository.save(new Customer(2L, "Dave", "Pegg"));
  outbox.get().schedule(getClass()).publishCustomerCreatedEvent(1L);
  outbox.get().schedule(getClass()).publishCustomerCreatedEvent(2L);
}

void publishCustomerCreatedEvent(long id) {
  // Remote call here
}
```

Notice that with a DI framework like Spring in play, you can **self-invoke** on `getClass()` - invoke a method on the same class that's scheduling it.
