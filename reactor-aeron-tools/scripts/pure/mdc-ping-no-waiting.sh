#!/usr/bin/env bash

cd ../../

JAR_FILE=$(ls target |grep jar)

java \
    -cp target/${JAR_FILE}:target/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Daeron.threading.mode=SHARED \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.embeddedMediaDriver=true \
    -Dreactor.aeron.sample.exclusive.publications=true \
    -Dreactor.aeron.sample.messages=100000000 \
    -Dreactor.aeron.sample.messageLength=32 \
    -Dreactor.aeron.sample.idle.strategy=yielding \
    ${JVM_OPTS} reactor.aeron.demo.pure.MdcPingNoWaiting
