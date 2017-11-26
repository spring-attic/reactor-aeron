[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


# reactor-aeron

An implementation of Reactive Streams over Aeron supporting unicast mode of data sending.

### Building reactor-aeron jars ###
    ./gradlew jar

### Running unit tests ###
    ./gradlew test

### Getting it
- Snapshot : **0.7.0.BUILD-SNAPSHOT**  ( Java 8+ required )
- Milestone : **TBA**  ( Java 8+ required )

With Gradle from repo.spring.io or Maven Central repositories (stable releases only):
```groovy
    repositories {
      maven { url 'http://repo.spring.io/snapshot' }
      //maven { url 'http://repo.spring.io/milestone' }
      mavenCentral()
    }

    dependencies {
      compile "io.projectreactor.ipc:reactor-aeron:0.6.0.BUILD-SNAPSHOT"
    }
```

### License ###

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_
