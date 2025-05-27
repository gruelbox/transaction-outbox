# transaction-outbox-jooq

[![jOOQ on Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-jooq/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-jooq)
[![jOOQ Javadoc](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-jooq.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-jooq)
[![Latest snapshot](https://img.shields.io/github/v/tag/gruelbox/transaction-outbox?label=snapshot&sort=semver)](#development-snapshots)

Extension for [transaction-outbox-core](../README.md) which integrates with jOOQ for transaction management.

Like Transaction Outbox, jOOQ is intended to play nicely with any other transaction management approach, but provides its own as an option. If you are already using jOOQ's `TransactionProvider` via `DSLContext.transaction(...)` throughout your application, you can continue to do so with this extension.

jOOQ gives you the option to either use thread-local transaction management or explicitly pass a contextual `DSLContext` or `Configuration` down your stack. You can do the same thing with `TransactionOutbox`.

## Installation

### Stable releases

#### Maven

```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-jooq</artifactId>
  <version>6.0.609</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-jooq:6.0.609'
```

### Development snapshots

See [transactionoutbox-core](../README.md) for more information.

## Using thread-local transactions

### Configuration

First, configure jOOQ to use thread-local transaction management:

```java
var jooqConfig = new DefaultConfiguration();
var connectionProvider = new DataSourceConnectionProvider(dataSource);
jooqConfig.setConnectionProvider(connectionProvider);
jooqConfig.setSQLDialect(SQLDialect.H2);
jooqConfig.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider, true));
```

Now connect `JooqTransactionListener`, which is the bridge between jOOQ and `TransactionOutbox`, and create the `DSLContext`:

```java
var listener = JooqTransactionManager.createListener();
jooqConfig.set(listener);
var dsl = DSL.using(jooqConfig);
```

Finally create the `TransactionOutbox`:

```java
var outbox = TransactionOutbox.builder()
    .transactionManager(JooqTransactionManager.create(dsl, listener))
    .persistor(Persistor.forDialect(Dialect.MY_SQL_8))
    .build();
}
```

### Usage

You can now use jOOQ and Transaction Outbox together, assuming thread-bound transactions.

```java
dsl.transaction(() -> {
  customerDao.save(new Customer(1L, "Martin", "Carthy"));
  customerDao.save(new Customer(2L, "Dave", "Pegg"));
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(1L);
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(2L);
});
```

## Using explicit transaction context

If you prefer not to use thread-local transactions, you are already taking on the burden of passing jOOQ `Configuration`s or `DSLContext`s down your stack. This is supported with `TransactionOutbox`, but requires a little explanation.

### Configuration

Without the need to synchronise the thread context, setup is a bit easier:

```java
// Create the DSLContext and connect the listener
var dsl = DSL.using(dataSource, SQLDialect.H2);
dsl.configuration().set(JooqTransactionManager.createListener());

// Create the outbox
var outbox = TransactionOutbox.builder()
    .transactionManager(JooqTransactionManager.create(dsl))
    .persistor(Persistor.forDialect(Dialect.MY_SQL_8))
    .build();
```

### Usage

The call pattern in the thread-local context example above will now not work:

```java
outbox.schedule(MyClass.class).publishCustomerCreatedEvent(1L);
```

`TransactionOutbox` needs the currently active transaction context to write the database record. To do so, you need to change the scheduled method itself to receive a `Configuration`:

```java
void publishCustomerCreatedEvent(long id, Configuration cfg2) {
  cfg.dsl().insertInto(...)...
}
```

Then call accordingly:

```java
dsl.transaction(cfg1 -> {
  new CustomerDao(cfg1).save(new Customer(1L, "Martin", "Carthy"));
  new CustomerDao(cfg1).save(new Customer(2L, "Dave", "Pegg"));
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(1L, cfg1);
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(2L, cfg1);
});
```

In the example above, `cfg1` is the transaction context in which the request is written to the database, and `cfg2` is the context in which it is executed, which will be a different transaction at some later time. `cfg1` is stripped from the request before writing it to the database and replaced with `cfg2` at run time.

The reason for passing the `Configuration` in the scheduled method call itself (rather than the `schedule()` method call) is twofold:

1.  It is very common for tasks to need access to the transaction context _at the time they are run_ in order to participate in that transaction. That way, if any part of the outbox task is rolled back, any work we do inside it is also rolled back.
2.  If the method were not scheduled by `TransactionOutbox`, but instead called directly, it would need the `Configuration` passed to it anyway. By working this way we ensure that the API for calling directly or scheduled is the same, and therefore the two implementations are interchangeable.
