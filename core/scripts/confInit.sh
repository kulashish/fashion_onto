#!/usr/bin/env bash


# copy config in a hdfs
hadoop fs -put /opt/alchemy-core/current/conf/*.json /apps/alchemy/conf/
