#!/usr/bin/env bash

declare -a addresses=("10.200.5.44")

CERT_PATH=/home/artemvysochyn/Documents/ec2_private_key
SRC_PATH=/home/artemvysochyn/Workspace/scalecube-reactor-aeron
USER_NAME=ubuntu

cd $SRC_PATH

mvn clean install -DskipTests

for addr in ${addresses[@]}
do
    echo "####### Setting up for: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'sudo rm -rf /tmp/*'
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'mkdir -p /tmp/reactor-aeron/scripts/'
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'mkdir -p /tmp/reactor-aeron/target/lib'
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/target/*.jar $USER_NAME@$addr:/tmp/reactor-aeron/target/
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/target/lib/* $USER_NAME@$addr:/tmp/reactor-aeron/target/lib/
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/scripts/* $USER_NAME@$addr:/tmp/reactor-aeron/scripts/
done
