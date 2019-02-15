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
    -Dreactor.aeron.sample.idle.strategy=yielding \
    -Dreactor.aeron.sample.messageLength=16 \
    -Dreactor.aeron.sample.request=128 \
    -Dreactor.aeron.sample.frameCountLimit=16384 \
    -Daeron.mtu.length=16k \
    ${JVM_OPTS} reactor.aeron.demo.pure.MdcPingNoWaitingWithSimpleBackpressure
