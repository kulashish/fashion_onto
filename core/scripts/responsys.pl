#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time());
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time());
#print $date_with_zero . "\n";

chdir("/data/responsys/");

# getting SMS OPT OUT and DND files
system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_SMS_OPT_OUT_$date_with_zero" . "_* ;'");
system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_SMS_DELIVERED_$date_with_zero" . "_* ;'");

# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/sms_opt_out/daily/$date/");
system("hadoop fs -copyFromLocal 53699_SMS_OPT_OUT_$date_with_zero" . "_*.txt /data/input/responsys/sms_opt_out/daily/$date/53699_SMS_OPT_OUT_$date_with_zero" . ".txt");

system("hadoop fs -mkdir -p /data/input/responsys/sms_delivered/daily/$date/");
system("hadoop fs -copyFromLocal 53699_SMS_DELIVERED_$date_with_zero" . "_*.txt /data/input/responsys/sms_delivered/daily/$date/53699_SMS_DELIVERED_$date_with_zero" . ".txt");

system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c dndMerger");
system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c smsOptOutMerger");
