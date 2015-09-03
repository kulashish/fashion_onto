#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);
#print $date_with_zero . "\n";

chdir("/data/ad4push/");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_517_$date_with_zero.csv; bye\"");

# reactions files
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_517_$date_with_zero.csv; bye\"");

# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/ad4push/reactions_android/daily/$date/");
system("hadoop fs -copyFromLocal exportMessagesReactions_517_$date_with_zero.csv /data/input/ad4push/reactions_android/daily/$date/.");

system("hadoop fs -mkdir -p /data/input/ad4push/reactions_ios/daily/$date/");
system("hadoop fs -copyFromLocal exportMessagesReactions_515_$date_with_zero.csv /data/input/ad4push/reactions_ios/daily/$date/.");

system("hadoop fs -mkdir -p /data/input/ad4push/devices_android/daily/$date/");
system("sed '/^$/d' exportDevices_517_$date_with_zero.csv | sed -n 'H;g;/^[^\"]*\"[^\"]*\(\"[^\"]*\"[^\"]*\)*$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h' >exportDevices_517_$date_with_zero_cleaned.csv")
system("hadoop fs -copyFromLocal exportDevices_517_$date_with_zero_cleaned.csv /data/input/ad4push/devices_android/daily/$date/exportDevices_517_$date_with_zero.csv");

system("hadoop fs -mkdir -p /data/input/ad4push/devices_ios/daily/$date/");
system("sed '/^$/d' /exportDevices_515_$date_with_zero.csv | sed -n 'H;g;/^[^\"]*\"[^\"]*\(\"[^\"]*\"[^\"]*\)*$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h' >exportDevices_515_$date_with_zero_cleaned.csv")
system("hadoop fs -copyFromLocal exportDevices_515_$date_with_zero_cleaned.csv /data/input/ad4push/devices_ios/daily/$date/exportDevices_515_$date_with_zero.csv");

# call ad4push pipeline
system("perl /opt/alchemy-core/current/bin/run.pl -t prod -c Ad4pushCustReact");

# copy processed data to ftp location
system("perl /opt/alchemy-core/current/bin/ftp_upload.pl -c ad4push_customer_response");

system("perl /opt/alchemy-core/current/bin/run.pl -t prod -c Ad4pushDeviceMerger");
