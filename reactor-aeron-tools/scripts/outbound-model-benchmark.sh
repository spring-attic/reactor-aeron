#!/usr/bin/env bash

cd $(dirname $0)
cd ../

JAR_FILE=$(ls target |grep jar)

java \
    -cp target/${JAR_FILE}:target/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Dreactor.aeron.sample.idle.strategy=yielding \
    -Dreactor.aeron.demo.outbound.concurrency=1 \
    -Dreactor.aeron.demo.outbound.prefetch=1 \
    -Dreactor.aeron.demo.outbound.fluxRepeat=100_000_000 \
    -Dreactor.aeron.demo.outbound.fluxThreads=1 \
    ${JVM_OPTS} reactor.aeron.demo.OutboundModelWithMonoProcessorBenchmarkRunner
