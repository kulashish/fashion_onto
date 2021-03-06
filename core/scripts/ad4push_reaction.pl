#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);
#print $date_with_zero . "\n";

chdir("/data/ad4push/");
# reactions files
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_517_$date_with_zero.csv; bye\"");

# copy data to hdfs
system("hadoop fs -mkdir -p /data/input/ad4push/reactions_android/daily/$date/");
system("hadoop fs -copyFromLocal exportMessagesReactions_517_$date_with_zero.csv /data/input/ad4push/reactions_android/daily/$date/.");

system("hadoop fs -mkdir -p /data/input/ad4push/reactions_ios/daily/$date/");
system("hadoop fs -copyFromLocal exportMessagesReactions_515_$date_with_zero.csv /data/input/ad4push/reactions_ios/daily/$date/.");

# call ad4push customer reaction
system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c ad4pushCustomerResponse");

# copy processed data to ftp location
system("perl /opt/alchemy-core/current/bin/ftp_upload.pl -t PROD -c ad4push_customer_response");
