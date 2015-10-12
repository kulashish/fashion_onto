#!/bin/sh

Day=1
mergeDir=$1

LY=`date -d "$Day day ago" +'%Y'`
LM=`date -d "$Day day ago" +'%-m'`
LD=`date -d "$Day day ago" +'%-d'`
dater=$LY"-"$LM"-"$LD

echo "LAST_YEAR=$LY"
echo "LAST_MONTH=$LM"
echo "LAST_DAY=$LD"
echo "DATER=$dater"

