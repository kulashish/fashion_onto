#!/bin/sh


ftp_date=$1
FTP_HOST=$2
FTP_USER=$3
FTP_PASSWD=$4
REMOTE_PATH=$5
DailyHiveOutput=$6

mkdir -p /tmp/reports/productImpressions
mkdir -p /tmp/reports/productViews


hadoop fs -copyToLocal $DailyHiveOutput/reports/productImpressions/* /tmp/reports/productImpressions
hadoop fs -copyToLocal $DailyHiveOutput/reports/productViews/* /tmp/reports/productViews

mkdir -p /tmp/priority_reports/

cat /tmp/reports/productImpressions/000* > /tmp/priority_reports/desktop_product_impressions_$ftp_date.csv
cat /tmp/reports/productViews/000* > /tmp/priority_reports/desktop_product_views_$ftp_date.csv
cd /tmp/priority_reports/


ftp -n -v $FTP_HOST <<END
user $FTP_USER $FTP_PASSWD
prompt
binary
cd $REMOTE_PATH
put desktop_product_impressions_$ftp_date.csv
put desktop_product_views_$ftp_date.csv
bye
END

rm -rf /tmp/reports
rm -rf /tmp/priority_reports
