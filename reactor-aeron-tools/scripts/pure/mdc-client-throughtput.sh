#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname $0)
TARGET_DIR="${SCRIPTS_DIR%/*}"/target
JAR_FILE=$(ls ${TARGET_DIR} |grep jar)

${JAVA_HOME}/bin/java \
    -cp ${TARGET_DIR}/${JAR_FILE}:${TARGET_DIR}/lib/* \
    -XX:BiasedLockingStartupDelay=0 \
    -Djava.net.preferIPv4Stack=true \
    -Daeron.term.buffer.sparse.file=false \
    -Daeron.threading.mode=SHARED \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.embeddedMediaDriver=true \
    -Dreactor.aeron.sample.exclusive.publications=true \
    -Dreactor.aeron.sample.idle.strategy=yielding \
    -Dreactor.aeron.sample.messageLength=2048 \
    -Daeron.mtu.length=16k \
    -Daeron.socket.so_sndbuf=2m \
    -Daeron.socket.so_rcvbuf=2m \
    -Daeron.rcv.initial.window.length=2m \
    ${JVM_OPTS} reactor.aeron.demo.pure.ClientThroughput
