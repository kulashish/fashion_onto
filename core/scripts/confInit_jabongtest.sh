#!/usr/bin/env bash

# copy config in a hdfs
hadoop fs -rm -r -skipTrash /apps/test/alchemy/conf/*.json
hadoop fs -put /opttest/alchemy-core/base/config.json /apps/test/alchemy/conf/
hadoop fs -put /opttest/alchemy-core/current/conf/*.json /apps/test/alchemy/conf/

# copy workflow files to hdfs
hadoop fs -rm -r -skipTrash /apps/test/alchemy/workflows/*
hadoop fs -put /opttest/alchemy-core/current/workflows/jabongtest/* /apps/test/alchemy/workflows/
hadoop fs -put /opttest/alchemy-core/current/workflows/prod/campaigns/workflow.xml /apps/test/alchemy/workflows/campaigns/.

# copying lib to hdfs
hadoop fs -mkdir -p /apps/test/alchemy/workflows/lib/
hadoop fs -put /opttest/alchemy-core/current/jar/Alchemy-assembly.jar /apps/test/alchemy/workflows/lib/.

#copying run.pl to hdfs
hadoop fs -mkdir -p /apps/test/alchemy/workflows/scripts/
hadoop fs -put /opttest/alchemy-core/current/bin/run.pl /apps/test/alchemy/workflows/scripts/.
