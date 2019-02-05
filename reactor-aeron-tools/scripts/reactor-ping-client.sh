#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname $0)
TARGET_DIR="${SCRIPTS_DIR%/*}"/target
JAR_FILE=$(ls ${TARGET_DIR} |grep jar)

${JAVA_HOME}/bin/java \
    -cp ${TARGET_DIR}/${JAR_FILE}:${TARGET_DIR}/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Daeron.threading.mode=SHARED \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.idle.strategy=backoff/0/0/0/1 \
    -Dreactor.aeron.sample.messages=100000000 \
    -Dreactor.aeron.sample.messageLength=32 \
    -Dreactor.aeron.sample.frameCountLimit=4 \
    -Dreactor.aeron.sample.report.interval=5 \
    -Dreactor.aeron.sample.request=8 \
    ${JVM_OPTS} reactor.aeron.demo.AeronPingClient
