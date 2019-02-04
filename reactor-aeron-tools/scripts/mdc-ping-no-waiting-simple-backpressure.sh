#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname $0)
TARGET_DIR="${SCRIPTS_DIR%/*}"/target
JAR_FILE=$(ls ${TARGET_DIR} |grep jar)

${JAVA_HOME}/bin/java \
    -cp ${TARGET_DIR}/${JAR_FILE}:${TARGET_DIR}/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Dreactor.aeron.sample.messages=1000000 \
    -Dreactor.aeron.sample.embeddedMediaDriver=true \
    -Dreactor.aeron.sample.request=8 \
    -Dreactor.aeron.sample.messageLength=32 \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.exclusive.publications=true \
    ${JVM_OPTS} reactor.aeron.demo.pure.MdcPingNoWaitingWithSimpleBackpressure
