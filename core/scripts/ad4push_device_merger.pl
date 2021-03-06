#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);
#print $date_with_zero . "\n";

chdir("/data/ad4push/");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_517_$date_with_zero.csv; bye\"");

# copy data to hdfs
my $regex2 = "'H;g;/^[^\"]*\"[^\"]*\(\"[^\"]*\"[^\"]*)*\$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h'";
system("hadoop fs -mkdir -p /data/input/ad4push/devices_android/daily/$date/");
system("sed '/^\$/d' exportDevices_517_$date_with_zero.csv | sed -n $regex2 >cleaned/exportDevices_517_$date_with_zero.csv");
system("hadoop fs -copyFromLocal cleaned/exportDevices_517_$date_with_zero.csv /data/input/ad4push/devices_android/daily/$date/.");

system("hadoop fs -mkdir -p /data/input/ad4push/devices_ios/daily/$date/");
system("sed '/^\$/d' exportDevices_515_$date_with_zero.csv | sed -n $regex2 >cleaned/exportDevices_515_$date_with_zero.csv");
system("hadoop fs -copyFromLocal cleaned/exportDevices_515_$date_with_zero.csv /data/input/ad4push/devices_ios/daily/$date/.");

# call ad4push Device Merger
system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c ad4pushDeviceMerger");

# copy processed data to ftp location
system("perl /opt/alchemy-core/current/bin/ftp_upload.pl -t PROD -c ad4push_device_merger");
