#!/bin/bash

BROKERS=<broker-ip>:9092
TOPIC=streamdata

echo "Listing topic $TOPIC"
rpk topic list --brokers $BROKERS

echo "Deleting topic" $TOPIC
rpk topic delete $TOPIC --brokers $BROKERS

echo "Creating topic" $TOPIC
rpk topic create $TOPIC -p 16 --brokers $BROKERS

rpk topic describe $TOPIC --brokers $BROKERS
