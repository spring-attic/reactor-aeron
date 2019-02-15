#!/usr/bin/env bash

cd ../../

JAR_FILE=$(ls target |grep jar)

java \
    -cp target/${JAR_FILE}:target/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Dreactor.aeron.sample.messages=100000000 \
    -Dreactor.aeron.sample.messageLength=16 \
    -Dreactor.aeron.sample.request=128 \
    ${JVM_OPTS} reactor.aeron.demo.rsocket.RsocketTcpPing
