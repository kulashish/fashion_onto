#!/bin/sh

HdfsInputPath=$1
HiveOutput=$2
Day="1"

LY=`date -d "$Day day ago" +'%Y'`
LM=`date -d "$Day day ago" +'%-m'`
LD=`date -d "$Day day ago" +'%-d'`
dater=$LY"-"$LM"-"$LD

HdfsBsonData=$HdfsInputPath/bson/$LY/$LM/$LD
HdfsInputPath=$HdfsInputPath/tmp/$LY/$LM/$LD
DailyHiveOutput=$HiveOutput/$LY/$LM/$LD


echo "LAST_YEAR=$LY"
echo "LAST_MONTH=$LM"
echo "LAST_DAY=$LD"
echo "DATER=$dater"
echo "HdfsBsonData=$HdfsBsonData"
echo "HdfsInputPath=$HdfsInputPath"
echo "DailyHiveOutput=$DailyHiveOutput"
