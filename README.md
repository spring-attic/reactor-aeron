# Reactor Aeron

`Reactor Aeron` offers non-blocking and backpressure-ready relable `UDP`
clients & servers based on `Aeron` framework.

## Getting it
`Reactor Aeron` requires Java 8 or + to run.

With `Maven` from `Maven Central` repositories (stable releases only):

```xml
<!-- https://mvnrepository.com/artifact/io.scalecube/scalecube-reactor-aeron -->
<dependency>
    <groupId>io.scalecube</groupId>
    <artifactId>scalecube-reactor-aeron</artifactId>
    <version>x.y.z</version>
    <type>pom</type>
</dependency>

```

## Getting Started

Here is a very simple server and the corresponding client example

```java
  AeronResources resources = new AeronResources().useTmpDir().start().block();

  AeronServer.create(resources)
    .options("localhost", 13000, 13001)
    .handle(
        connection ->
            connection
                .inbound()
                .receive()
                .asString()
                .log("receive")
                .then(connection.onDispose()))
    .bind()
    .block()
    .onDispose(resources)
    .onDispose()
    .block();
    
```

```java
    AeronResources resources = new AeronResources().useTmpDir().start().block();

    AeronClient.create(resources)
        .options("localhost", 13000, 13001)
        .handle(
            connection1 -> {
              System.out.println("Handler invoked");
              return connection1
                  .outbound()
                  .sendString(Flux.fromStream(Stream.of("Hello", "world!")).log("send"))
                  .then(connection1.onDispose());
            })
        .connect()
        .block()
        .onDispose(resources)
        .onDispose()
        .block();
```

## Building from Source

```console
$ git clone git@github.com:scalecube/reactor-aeron.git
$ cd reactor-aeron
$ mvn clean install
```

## [Code style](https://github.com/scalecube/scalecube-parent/blob/develop/DEVELOPMENT.md#setting-up-development-environment) 
