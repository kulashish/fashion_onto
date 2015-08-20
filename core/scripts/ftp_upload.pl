#!/usr/bin/env perl


use strict;
use 5.010;
use warnings;
use Getopt::Long qw(GetOptions);
Getopt::Long::Configure qw(gnu_getopt);
use Data::Dumper;
use Time::HiRes qw( time );
use Mail::Sendmail;

my $debug;
my $component;

GetOptions (
    'component|c=s' => \$component,
    'debug|d' => \$debug,
) or die "Usage: $0 --debug --component|-c campaigns | ad4push_customer_response | dcf_feed\n";


use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);
print $date_with_zero . "\n";

my $date_with_hiphen = strftime "%Y-%m-%d", localtime(time() - 60*60*24);
print $date_with_hiphen . "\n";

if ($component eq "campaigns") {
    uploadCampaign();
} elsif ($component eq "ad4push_customer_response") {
    upload_ad4push_customer_response();
} elsif ($component eq "dcf_feed") {
    upload_dcf_feed();
}


# upload ad4push customer response files
# /data/tmp/ad4push/reactions_android_csv/full/2015/07/30/24/ad4push_customer_response_android_20150730.csv
# /data/tmp/ad4push/reactions_ios_csv/full/2015/07/30/24/ad4push_customer_response_ios_20150730.csv

sub fetchCampaign {
   my ($id, $cname, $base) = @_;
   #system("hadoop fs -get /data/tmp/campaigns/$cname" . "_$id/daily/$date/staticlist_$cname" . "_$id" . "_$date_with_zero.csv $base/");
   my $name = "staticlist_$cname" . "_$id" . "_$date_with_zero.csv";
   my $nametxt = "staticlist_$cname" . "_$id" . "_$date_with_zero.txt";

   #my $tmpName = "$base/tmp/staticlist_$cname" . "_$id" . "_$date_with_zero.csv";
   system("hadoop fs -get /data/tmp/campaigns/$cname" . "_$id/daily/$date/$name $base/$name");
   system("touch $base/$nametxt");

   #system("sed 1i'\"deviceId\"' $tmpName | sed 's/\(\[\|\]\)//g' > $name");

}


sub uploadCampaign {
    my $base = "/data/export/$date_with_zero/campaigns";
    print "directory is $base\n";
    system("mkdir -p $base");
    #system("mkdir -p $base/tmp");


    # /data/tmp/campaigns/acart_daily42_515/daily/2015/07/30/staticlist_acart_daily42_515_20150730.csv
    for (my $i = 0; $i <= 1 ; $i++) {
       my $id = "515";  # ios
       if ($i == 1) {
           $id = "517";
           system("hadoop fs -get /data/tmp/campaigns/android/daily/$date/updateDevices_$id" . "_$date_with_zero.csv $base/");
           system("touch $base/updateDevices_$id" . "_$date_with_zero.txt");

           #system("hadoop fs -get /data/tmp/campaigns/android/daily/$date/ $base/UpdateDevices_$id" . "_$date_with_zero.csv");
           
       } else {
           system("hadoop fs -get /data/tmp/campaigns/ios/daily/$date/updateDevices_$id" . "_$date_with_zero.csv $base/");
           system("touch $base/updateDevices_$id" . "_$date_with_zero.txt");

           #system("hadoop fs -get /data/tmp/campaigns/ios/daily/$date/part-00000 $base/UpdateDevices_$id" . "_$date_with_zero.csv");

       }

       # master file

       # acart daily
       my $cname = "acart_daily42";
       fetchCampaign($id, $cname, $base);

       $cname = "acart_followup43";
       fetchCampaign($id, $cname, $base);

       $cname = "acart_iod45";
       fetchCampaign($id, $cname, $base);

       $cname = "acart_lowstock44";
       fetchCampaign($id, $cname, $base);

       $cname = "cancel_retarget46";
       fetchCampaign($id, $cname, $base);

       $cname = "return_retarget47";
       fetchCampaign($id, $cname, $base);

       $cname = "invalid_followup48";
       fetchCampaign($id, $cname, $base);

       $cname = "invalid_lowstock49";
       fetchCampaign($id, $cname, $base);

       $cname = "wishlist_followup53";
       fetchCampaign($id, $cname, $base);

       $cname = "wishlist_iod54";
       fetchCampaign($id, $cname, $base);

       $cname = "wishlist_lowstock55";
       fetchCampaign($id, $cname, $base);

       $cname = "surf156";
       fetchCampaign($id, $cname, $base);

       $cname = "surf257";
       fetchCampaign($id, $cname, $base);

       $cname = "surf358";
       fetchCampaign($id, $cname, $base);

       $cname = "surf671";
       fetchCampaign($id, $cname, $base);
    }
       
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_campaigns/ $base/*; bye\"");
    system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  mput -O imports/ $base/*; bye\"");
}

sub upload_ad4push_customer_response {
    my $base = "/data/export/$date_with_zero/ad4push_response";
    print "ad4push customer response directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/ad4push/customer_response/full/2015/07/30/part-00000
   print "hadoop fs -get /data/tmp/ad4push/customer_response/full/$date/ad4push_customer_response_$date_with_zero.csv $base/\n";

   # /data/tmp/ad4push/customer_response/full/2015/07/30/part-00000
   system("hadoop fs -get /data/tmp/ad4push/customer_response/full/$date/ad4push_customer_response_$date_with_zero.csv $base/");

   system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_customer_response/ $base/*; bye\"");
   system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  mput -O imports/ $base/*; bye\"");

}

sub upload_dcf_feed {
     my $base = "/data/export/$date_with_zero/dcf_feed/clickstream_merged_feed";
     print "dcf feed directory is $base\n";
     system("mkdir -p $base");

     print "hadoop fs -get /data/tmp/dcf_feed/clickstream_merged_feed/full/$date/webhistory_$date_with_hiphen.csv $base/\n";

     system("hadoop fs -get /data/tmp/dcf_feed/clickstream_merged_feed/full/$date/webhistory_$date_with_hiphen.csv $base/)";

     print("gzip $base/webhistory_$date_with_hiphen.csv/\n")
     system("gzip $base/webhistory_$date_with_hiphen.csv");

     system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O dcf_feed/ $base/webhistory_$date_with_hiphen.csv.gz; bye\"");
}