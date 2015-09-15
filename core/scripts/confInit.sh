#!/usr/bin/env bash

# copy config in a hdfs
hadoop fs -rm -r /apps/alchemy/conf/*.json
hadoop fs -put /opt/alchemy-core/current/conf/*.json /apps/alchemy/conf/
hadoop fs -put /opt/alchemy-core/base/config.json /apps/alchemy/conf/
hadoop fs -rm -r /apps/alchemy/workflows/*
hadoop fs -put /opt/alchemy-core/current/workflows/* /apps/alchemy/workflows/

