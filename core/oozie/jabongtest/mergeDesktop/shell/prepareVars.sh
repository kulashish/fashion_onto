#!/bin/sh


DesktopMergeOutput=$1
Day=1

LY=`date -d "$Day day ago" +'%Y'`
LM=`date -d "$Day day ago" +'%-m'`
LD=`date -d "$Day day ago" +'%-d'`
dater=$LY"-"$LM"-"$LD

DesktopMergeOutput=$DesktopMergeOutput/$LY/$LM/$LD

ftp_date=`date -d"$Day day ago" +"%Y%m%d"`


echo "LAST_YEAR=$LY"
echo "LAST_MONTH=$LM"
echo "LAST_DAY=$LD"
echo "DATER=$dater"
echo "ftp_date=$ftp_date"
echo "DesktopMergeOutput=$DesktopMergeOutput"
