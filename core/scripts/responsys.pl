#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time());
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time());
#print $date_with_zero . "\n";

my $date_with_zero_yesterday = strftime "%Y%m%d", localtime(time() - 60*60*24);
#print $date_with_zero_yesterday . "\n";


chdir("/data/responsys/");

# getting SMS OPT OUT and DND files
system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_SMS_OPT_OUT_$date_with_zero" . "_* ;'");
# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/sms_opt_out/daily/$date/");
system("hadoop fs -copyFromLocal 53699_SMS_OPT_OUT_$date_with_zero" . "_*.txt /data/input/responsys/sms_opt_out/daily/$date/53699_SMS_OPT_OUT_$date_with_zero" . ".txt");

system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_SMS_DELIVERED_$date_with_zero" . "_* ;'");
# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/sms_delivered/daily/$date/");
system("hadoop fs -copyFromLocal 53699_SMS_DELIVERED_$date_with_zero" . "_*.txt /data/input/responsys/sms_delivered/daily/$date/53699_SMS_DELIVERED_$date_with_zero" . ".txt");

system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c dndMerger");
system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c smsOptOutMerger");

#copying to test folder for jabongtest run of contactListMobile.
system("hadoop fs -cp /data/output/responsys/sms_opt_out/full/$date /data/test/output/responsys/sms_opt_out/full/$date");
system("hadoop fs -cp /data/output/responsys/DND/full/$date /data/test/output/responsys/DND/full/$date");
system("hadoop fs -cp /data/output/solutionsInfiniti/block_list_numbers/full/$date /data/test/output/solutionsInfiniti/block_list_numbers/full/$date");

system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_OPEN_$date_with_zero" . "_* ;'");
# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/open/daily/$date/");
system("hadoop fs -copyFromLocal 53699_OPEN_$date_with_zero" . "_*.txt /data/input/responsys/open/daily/$date/53699_OPEN_$date_with_zero" . ".txt");

system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_BOUNCE_$date_with_zero" . "_* ;'");
# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/bounce/daily/$date/");
system("hadoop fs -copyFromLocal 53699_BOUNCE_$date_with_zero" . "_*.txt /data/input/responsys/bounce/daily/$date/53699_BOUNCE_$date_with_zero" . ".txt");

system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget download/53699_CLICK_$date_with_zero" . "_* ;'");
# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/responsys/click/daily/$date/");
system("hadoop fs -copyFromLocal 53699_CLICK_$date_with_zero" . "_*.txt /data/input/responsys/click/daily/$date/53699_CLICK_$date_with_zero" . ".txt");

system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget archive/53699_33838_$date_with_zero_yesterday" . "_LIVE_CAMPAIGN.csv.zip ;'");

