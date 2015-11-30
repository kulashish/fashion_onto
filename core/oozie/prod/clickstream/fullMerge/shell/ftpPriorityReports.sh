#!/bin/sh


ftp_date=$1
FTP_HOST=$2
FTP_USER=$3
FTP_PASSWD=$4
REMOTE_PATH=$5
DailyHiveOutput=$6

mkdir -p /tmp/reports/appProductImpressions
mkdir -p /tmp/reports/appProductViews
mkdir -p /tmp/reports/desktopProductImpressions
mkdir -p /tmp/reports/desktopProductViews


hadoop fs -copyToLocal $DailyHiveOutput/reports/appProductImpressions/* /tmp/reports/appProductImpressions
hadoop fs -copyToLocal $DailyHiveOutput/reports/appProductViews/* /tmp/reports/appProductViews
hadoop fs -copyToLocal $DailyHiveOutput/reports/desktopProductImpressions/* /tmp/reports/desktopProductImpressions
hadoop fs -copyToLocal $DailyHiveOutput/reports/desktopProductViews/* /tmp/reports/desktopProductViews

mkdir -p /tmp/priority_reports/

cat /tmp/reports/appProductImpressions/000* > /tmp/priority_reports/app_product_impressions_$ftp_date.csv
cat /tmp/reports/appProductViews/000* > /tmp/priority_reports/app_product_views_$ftp_date.csv
cat /tmp/reports/desktopProductImpressions/000* > /tmp/priority_reports/desktop_product_impressions_$ftp_date.csv
cat /tmp/reports/desktopProductViews/000* > /tmp/priority_reports/desktop_product_views_$ftp_date.csv

cd /tmp/priority_reports/


ftp -n -v $FTP_HOST <<END
user $FTP_USER $FTP_PASSWD
prompt
binary
cd $REMOTE_PATH
put app_product_impressions_$ftp_date.csv
put app_product_views_$ftp_date.csv
put desktop_product_impressions_$ftp_date.csv
put desktop_product_views_$ftp_date.csv
bye
END

rm -rf /tmp/reports
rm -rf /tmp/priority_reports
