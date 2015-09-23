#!/usr/bin/env bash

# copy config in a hdfs
hadoop fs -rm -r -skipTrash /apps/alchemy/conf/*.json
hadoop fs -put /opt/alchemy-core/base/config.json /apps/alchemy/conf/
hadoop fs -put /opt/alchemy-core/current/conf/*.json /apps/alchemy/conf/

# copy workflow files to hdfs
hadoop fs -rm -r -skipTrash /apps/alchemy/workflows/*
hadoop fs -put /opt/alchemy-core/current/workflows/prod/* /apps/alchemy/workflows/

# copying lib to hdfs
hadoop fs -mkdir -p /apps/alchemy/workflows/lib/
hadoop fs -put /opt/alchemy-core/current/jar/Alchemy-assembly.jar /apps/alchemy/workflows/lib/.

#copying run.pl to hdfs
hadoop fs -mkdir -p /apps/alchemy/workflows/scripts/
hadoop fs -put /opt/alchemy-core/current/bin/run.pl /apps/alchemy/workflows/scripts/.
