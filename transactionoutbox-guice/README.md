# transaction-outbox-guice

[![Guice on Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-guice/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-guice)
[![Guice Javadoc](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-guice.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-guice)
[![Latest snapshot](https://img.shields.io/github/v/tag/gruelbox/transaction-outbox?label=snapshot&sort=semver)](#development-snapshots)

Extension for [transaction-outbox-core](../README.md) which integrates with Guice.

## Installation

### Stable releases

#### Maven

```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-guice</artifactId>
  <version>6.0.609</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-guice:6.0.609'
```

### Development snapshots

See [transactionoutbox-core](../README.md) for more information.

## Standard usage

### Configuration

To get a `TransactionOutbox` for use throughout your application, add a `Singleton` binding for your chosen transaction manager and then wire in `GuiceInstantiator` as follows:

```java
@Provides
@Singleton
TransactionOutbox transactionOutbox(Injector injector, TransactionManager transactionManager) {
  return TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .persistor(Persistor.forDialect(Dialect.MY_SQL_8))
    .instantiator(GuiceInstantiator.builder().injector(injector).build())
    .build();
}
```

### Usage

```java
@Inject private TransactionOutbox outbox;

void doSomething() {
  // Obtains a MyService from the injector
  outbox.schedule(MyService.class).doAThing(1, 2, 3);
}
```

## Remote injection

Alternatively, you may prefer to hide the use of `TransactionOutbox` and create injectable "remote" implementations of specific services. This is a stylistic choice, and is more a Guice thing than a `TransactionOutbox` thing, but is presented here for illustration.

### Configuration

Create a suitable binding annotation to specify that you want to inject the remote version of a service:

```java
@Target({ElementType.PARAMETER, ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface Remote {}
```

Bind `TransactionOutbox` as per the example above, and add two more bindings to expose the "real" and "remote" versions of the service:

```java
@Provides
@Remote
@Singleton // Can help performance
MyService remote(TransactionOutbox outbox) {
  return outbox.schedule(MyService.class);
}

@Provides
MyService local() {
  return new MyService();
}
```

### Usage

Now you can inject the remote implementation and use it to schedule work. The following is exactly equivalent to the usage example above, just using an injected remote:

```java
@Inject
@Remote
private MyService myService;

void doSomething() {
  myService.doAThing(1, 2, 3);
}
```
