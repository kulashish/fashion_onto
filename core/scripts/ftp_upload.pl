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
my $target;
GetOptions (
    'target|t=s' => \$target,
    'component|c=s' => \$component,
    'debug|d' => \$debug,
) or die "Usage: $0 --debug --component|-c campaigns | ad4push_customer_response | dcf_feed | pricing_sku_data\n";

if ($target ne "PROD") {
    print "Will upload files only for PROD\n";
    exit -1;
}

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);
print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time() - 60*60*24);
print $date_with_zero . "\n";

my $date_with_hiphen = strftime "%Y-%m-%d", localtime(time() - 60*60*24);
print $date_with_hiphen . "\n";

my $date_with_zero_today = strftime "%Y%m%d", localtime(time());
print $date_with_zero_today . "\n";

if ($component eq "campaigns") {
    uploadCampaign();
} elsif ($component eq "ad4push_customer_response") {
    upload_ad4push_customer_response();
} elsif ($component eq "ad4push_device_merger") {
    upload_ad4push_device_merger();
} elsif ($component eq "dcf_feed") {
    upload_dcf_feed();
} elsif ($component eq "pricing_sku_data") {
    upload_pricing_sku_data();
} elsif ($component eq "custWelcomeVoucher") {
    upload_email_campaigns_custWelcomeVoucher();
} elsif ($component eq "custPreference") {
    upload_email_campaigns_custPreference();
} elsif ($component eq "contactListMobile") {
    upload_email_campaigns_contactListMobile();
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
    my $status = $?;
    system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  mput -O imports/ $base/*; bye\"");
    $status ||= $?;
    system("rm -rf $base");
    return $status;
}

sub upload_ad4push_customer_response {
    my $base = "/data/export/$date_with_zero/ad4push_response";
    print "ad4push customer response directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/ad4push/customer_response/full/2015/07/30/part-00000
   print "hadoop fs -get /data/tmp/ad4push/customer_response/full/$date/ad4push_customer_response_$date_with_zero.csv $base/\n";

   # /data/tmp/ad4push/customer_response/full/2015/07/30/part-00000
   system("hadoop fs -get /data/tmp/ad4push/customer_response/full/$date/ad4push_customer_response_$date_with_zero.csv $base/");
   my $status = $?;
   system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_customer_response/ $base/*; bye\"");
   # system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  mput -O imports/ $base/*; bye\"");
   $status ||= $?;
   system("rm -rf $base");
   return $status;
}

sub upload_ad4push_device_merger {
    my $base = "/data/export/$date_with_zero/ad4push_devices";
    print "ad4push devices directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/ad4push/devices_android/full/2015/09/02/24/exportDevices_517_20150902.csv
   print "hadoop fs -get /data/tmp/ad4push/devices_android/full/$date/24/exportDevices_517_$date_with_zero.csv $base/\n";

   # /data/tmp/ad4push/devices_android/full/2015/09/02/24/exportDevices_517_20150902.csv
   system("hadoop fs -get /data/tmp/ad4push/devices_android/full/$date/24/exportDevices_517_$date_with_zero.csv $base/");
   my $status = $?;
   # /data/tmp/ad4push/devices_ios/full/2015/09/02/24/exportDevices_515_20150902.csv
   print "hadoop fs -get /data/tmp/ad4push/devices_ios/full/$date/24/exportDevices_515_$date_with_zero.csv $base/\n";

   # /data/tmp/ad4push/devices_ios/full/2015/09/02/24/exportDevices_515_20150902.csv
   system("hadoop fs -get /data/tmp/ad4push/devices_ios/full/$date/24/exportDevices_515_$date_with_zero.csv $base/");
   $status ||= $?;
   system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_devices_merge/ $base/*; bye\"");
   $status ||= $?;
   system("rm -rf $base");
   return $status;
}

sub upload_dcf_feed {
     my $base = "/data/export/$date_with_zero/dcf_feed/clickstream_merged_feed";
     print "dcf feed directory is $base\n";
     system("mkdir -p $base");

     print "hadoop fs -get /data/tmp/dcf_feed/clickstream_merged_feed/full/$date/webhistory_$date_with_hiphen"."_1.csv $base/\n";

     system("hadoop fs -get /data/tmp/dcf_feed/clickstream_merged_feed/full/$date/webhistory_$date_with_hiphen"."_1.csv $base/");
     my $status = $?;
     dcf_file_format_change("$base/webhistory_$date_with_hiphen"."_1.csv","$base/webhistory_$date_with_hiphen.csv");
     print("gzip $base/webhistory_$date_with_hiphen.csv\n");
     system("gzip $base/webhistory_$date_with_hiphen.csv");

     system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O dcf_feed/ $base/webhistory_$date_with_hiphen.csv.gz; bye\"");
     $status ||= $?;
     system("lftp -c \"open -u shortlistdump,dumpshortlist 54.254.101.71 ;  mput -O webhistory_data/ $base/webhistory_$date_with_hiphen.csv.gz; bye\"");
     $status ||= $?;
     system("rm -rf $base");
     return $status;
}

sub upload_email_campaigns_custWelcomeVoucher {
    my $base = "/data/export/$date_with_zero/custWelcomeVoucher";
    print "custWelcomeVoucher directory is $base\n";
    system("mkdir -p $base");

    # 53699_28346_20150927_CUST_WELCOME_VOUCHERS.csv
    my $filename = "53699_28346_$date_with_zero_today"."_CUST_WELCOME_VOUCHERS.csv";

    # /data/tmp/variables/custWelcomeVoucher/daily/2015/09/26/53699_28346_20150927_CUST_WELCOME_VOUCHERS.csv
    print "hadoop fs -get /data/tmp/variables/custWelcomeVoucher/daily/$date/$filename $base/\n";

    # /data/tmp/variables/custWelcomeVoucher/daily/2015/09/26/53699_28346_20150927_CUST_WELCOME_VOUCHERS.csv
    system("hadoop fs -get /data/tmp/variables/custWelcomeVoucher/daily/$date/$filename $base/");
    my $status = $?;
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/$filename ; bye\"");
    $status ||= $?;
    system("rm -rf $base");
    return $status;
}

sub upload_email_campaigns_custPreference {
    my $base = "/data/export/$date_with_zero/custPreference";
    print "custWelcomeVoucher directory is $base\n";
    system("mkdir -p $base");

    # 53699_28335_20150927_CUST_PREFERENCE.csv
    my $filename = "53699_28335_$date_with_zero_today"."_CUST_PREFERENCE.csv";

    # /data/tmp/variables/custPreference/daily/2015/09/26/53699_28335_20150927_CUST_PREFERENCE.csv
    print "hadoop fs -get /data/tmp/variables/custPreference/daily/$date/$filename $base/\n";

    # /data/tmp/variables/custPreference/daily/2015/09/26/53699_28335_20150927_CUST_PREFERENCE.csv
    system("hadoop fs -get /data/tmp/variables/custPreference/daily/$date/$filename $base/");
    my $status = $?;
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/$filename ; bye\"");
    $status ||= $status;
    system("rm -rf $base");
    return $status;
}

sub upload_email_campaigns_contactListMobile {
    my $base = "/data/export/$date_with_zero/contactListMobile";
    print "contactListMobile directory is $base\n";
    system("mkdir -p $base");

    # 53699_28334_20150928_CONTACTS_LIST_MOBILE.csv
    my $filename = "53699_28334_$date_with_zero_today"."_CONTACTS_LIST_MOBILE.csv";

    # /data/tmp/variables/contactListMobile/daily/2015/09/27/53699_28334_20150928_CONTACTS_LIST_MOBILE.csv
    print "hadoop fs -get /data/tmp/variables/contactListMobile/daily/$date/$filename $base/\n";

    # /data/tmp/variables/contactListMobile/daily/2015/09/27/53699_28334_20150928_CONTACTS_LIST_MOBILE.csv
    system("hadoop fs -get /data/tmp/variables/contactListMobile/daily/$date/$filename $base/");
    my $status = $?;
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/$filename ; bye\"");
    $status ||= $?;
    system("rm -rf $base");
    return $status;
}

sub dcf_file_format_change{
    my ($file_input,$file_output) = (shift,shift);
    open(my $dcf_output, '>>', $file_output);
    print  $dcf_output "uid,sku,date_created,sessionId\n";
    open(my $fh, '<:encoding(UTF-8)', $file_input);
    while( my $line = <$fh>)  {
        chomp($line);
        my @words = split /,/, $line;
        if($words[0] eq "0" or $words[0] eq "null"){
            print $dcf_output ",\"$words[1]\",$words[2],\"$words[3]\"\n";
        }
        else{
            print $dcf_output "\"$words[0]\",\"$words[1]\",$words[2],\"$words[3]\"\n";
        }
    }
 }

sub upload_pricing_sku_data {
    my $base = "/data/export/$date_with_zero/pricing_sku_data";
    print "pricing sku data directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/sku_data/pricing/daily/2015/08/19/part-00000
   print "hadoop fs -get /data/tmp/sku_data/pricing/daily/$date/sku_data_pricing_$date_with_zero.csv $base/\n";

   # /data/tmp/sku_data/pricing/daily/2015/08/19/part-00000
   system("hadoop fs -get /data/tmp/sku_data/pricing/daily/$date/sku_data_pricing_$date_with_zero.csv $base/");
   my $status = $?;
   # gzipping the file
   system("gzip -c /data/export/$date_with_zero/pricing_sku_data/sku_data_pricing_$date_with_zero.csv >>/data/export/$date_with_zero/pricing_sku_data/$date_with_zero");
   # copying to slave location
   system("scp /data/export/$date_with_zero/pricing_sku_data/$date_with_zero dataplatform-slave4:/var/www/html/data/sku-pageview-summary/$date_with_zero");
   $status ||= $?;
   system("rm -rf $base");
   return $status;
}

