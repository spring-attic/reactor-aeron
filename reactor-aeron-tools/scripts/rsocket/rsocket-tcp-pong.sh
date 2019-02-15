#!/usr/bin/env bash

cd ../../

JAR_FILE=$(ls target |grep jar)

java \
    -cp target/${JAR_FILE}:target/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    ${JVM_OPTS} reactor.aeron.demo.rsocket.RsocketTcpPong
