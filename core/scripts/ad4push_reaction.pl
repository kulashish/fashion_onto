#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);

print $date_with_zero . "\n";

chdir("/data/ad4push/");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportDevices_517_$date_with_zero.csv; bye\"");

# reactions files
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_515_$date_with_zero.csv; bye\"");
system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  get  exports/exportMessagesReactions_517_$date_with_zero.csv; bye\"");

# copy data to hdfs
system("hadoop fs -copyFromLocal exportMessagesReactions_517_$date_with_zero.csv /data/input/ad4push/reactions_android/daily/$date/.")
system("hadoop fs -copyFromLocal exportMessagesReactions_515_$date_with_zero.csv /data/input/ad4push/reactions_ios/daily/$date/.")

# call ad4push pipeline
system("perl /opt/alchemy-core/current/bin/run.pl -t prod -c Ad4pushCustReact")