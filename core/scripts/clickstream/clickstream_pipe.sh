#!/bin/sh
LAST_YEAR=`date -d "$Day day ago" +'%Y'`
LAST_MONTH=`date -d "$Day day ago" +'%-m'`
LAST_DAY=`date -d "$Day day ago" +'%-d'`

MergeDir=/data/clickstream/merge

MergeDB=merge
MergeTable=merge_pagevisit

hadoop dfs -mkdir -p $MergeDir/$LAST_YEAR/$LAST_MONTH
hadoop distcp -pb hdfs://172.16.84.37:8020$MergeDir/$LAST_YEAR/$LAST_MONTH/$LAST_DAY hdfs://dataplatform-master:8020$MergeDir/$LAST_YEAR/$LAST_MONTH


hive -d loc=$MergeDir/$LAST_YEAR/$LAST_MONTH/$LAST_DAY/ -d dbname=$MergeDB -d tableName=$MergeTable -d year1=$LAST_YEAR -d month1=$LAST_MONTH -d date1=$LAST_DAY  -f /opt/alchemy-core/current/bin/clickstream/e2e-add-partition.q
perl /opt/alchemy-core/current/bin/run.pl -t prod -c clickstreamYesterdaySession
perl /opt/alchemy-core/current/bin/run.pl -t prod -c clickstreamSurf3Variable
