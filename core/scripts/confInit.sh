#!/usr/bin/env bash

while getopts ":t:" opt; do
  case $opt in
    t) target="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

HDFS_BASE = /apps/alchemy

if [$target = 'TEST-PROD']
then
  HDFS_BASE = /apps/test/alchemy
fi


# copy config in a hdfs
hadoop fs -rm -r -skipTrash $HDFS_BASE/conf/*.json
hadoop fs -put /opt/alchemy-core/base/config.json $HDFS_BASE/conf/
hadoop fs -put /opt/alchemy-core/current/conf/*.json $HDFS_BASE/conf/

# copy workflow files to hdfs
hadoop fs -rm -r -skipTrash /apps/alchemy/workflows/*
hadoop fs -put /opt/alchemy-core/current/workflows/* $HDFS_BASE/workflows/

# copying lib to hdfs
hadoop fs -mkdir -p /apps/alchemy/workflows/lib/
hadoop fs -put /opt/alchemy-core/current/jar/Alchemy-assembly.jar $HDFS_BASE/workflows/lib/.

#copying run.pl to hdfs
hadoop fs -mkdir -p /apps/alchemy/workflows/scripts/
hadoop fs -put /opt/alchemy-core/current/bin/run.pl $HDFS_BASE/workflows/scripts/.
