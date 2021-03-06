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
) or die "Usage: $0 --debug --component|-c push_campaigns | dcf_feed | pricing_sku_data | ad4push_customer_response | ad4push_device_merger | feedFiles | decryptFeedFiles | email_campaigns | calendar_campaigns | acart_hourly_campaign \n";

if ($target ne "PROD") {
    print "Will upload files only for PROD\n";
    exit 0;
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

my $date_today = strftime "%Y/%m/%d", localtime(time());
print $date_today . "\n";

my $current_hour = strftime "%H", localtime(time());
print $current_hour . "\n";

my $job_exit;

if ($component eq "push_campaigns") {
    $job_exit = upload_push_campaigns();
} elsif ($component eq "ad4push_customer_response") {
    $job_exit = upload_ad4push_customer_response();
} elsif ($component eq "ad4push_device_merger") {
    $job_exit = upload_ad4push_device_merger();
} elsif ($component eq "dcf_feed") {
    $job_exit = upload_dcf_feed();
} elsif ($component eq "pricing_sku_data") {
    $job_exit = upload_pricing_sku_data();
} elsif ($component eq "feedFiles") {
    $job_exit = upload_email_campaigns_feedFiles();
} elsif ($component eq "decryptFeedFiles") {
    $job_exit = upload_email_campaigns_decryptFeedFiles();
} elsif ($component eq "email_campaigns") {
    $job_exit = upload_email_campaigns();
} elsif ($component eq "calendar_campaigns") {
     $job_exit = upload_calendar_replenish_campaigns();
} elsif ($component eq "acart_hourly_campaign") {
     $job_exit = upload_acart_hourly_campaign();
} elsif ($component eq "customer_device_mapping_feed") {
     $job_exit = upload_customer_device_mapping_feed();
} else {
    print "not a valid component\n";
    $job_exit = -1;
}

exit $job_exit;

sub fetchCampaign {
   my ($id, $cname, $base) = @_;
   #system("hadoop fs -get /data/tmp/push_campaigns/$cname" . "_$id/daily/$date/staticlist_$cname" . "_$id" . "_$date_with_zero.csv $base/");
   my $name = "staticlist_$cname" . "_$id" . "_$date_with_zero.csv";
   my $nametxt = "staticlist_$cname" . "_$id" . "_$date_with_zero.txt";

   #my $tmpName = "$base/tmp/staticlist_$cname" . "_$id" . "_$date_with_zero.csv";
   system("hadoop fs -get /data/tmp/push_campaigns/$cname" . "_$id/daily/$date/$name $base/$name");
   system("touch $base/$nametxt");

   #system("sed 1i'\"deviceId\"' $tmpName | sed 's/\(\[\|\]\)//g' > $name");

}


sub upload_push_campaigns {
    my $base = "/tmp/$date_with_zero/push_campaigns";
    print "directory is $base\n";
    system("mkdir -p $base");
    #system("mkdir -p $base/tmp");

    # master file
    # /data/tmp/push_campaigns/acart_daily42_515/daily/2015/07/30/staticlist_acart_daily42_515_20150730.csv
    for (my $i = 0; $i <= 1 ; $i++) {
       my $id = "515";  # ios
       if ($i == 1) {
           $id = "517";
           system("hadoop fs -get /data/tmp/push_campaigns/android/daily/$date/updateDevices_$id" . "_$date_with_zero.csv $base/");
           system("touch $base/updateDevices_$id" . "_$date_with_zero.txt");
       } else {
           system("hadoop fs -get /data/tmp/push_campaigns/ios/daily/$date/updateDevices_$id" . "_$date_with_zero.csv $base/");
           system("touch $base/updateDevices_$id" . "_$date_with_zero.txt");
       }

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

    # system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_campaigns/ $base/*; bye\"");
    # my $status = $?;
    system("lftp -c \"open -u jabong,oJei-va8opue7jey sftp://sftp.ad4push.msp.fr.clara.net ;  mput -O imports/ $base/*; bye\"");
    my $status ||= $?;
    system("rm -rf /tmp/$date_with_zero");
    return $status;
}

sub upload_ad4push_customer_response {
    my $base = "/tmp/$date_with_zero/ad4push_response";
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
   system("rm -rf /tmp/$date_with_zero");
   return $status;
}

sub upload_ad4push_device_merger {
    my $base = "/tmp/$date_with_zero/ad4push_devices";
    print "ad4push devices directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/ad4push/devices_android/full/2015/09/02/24/exportDevices_517_20150902_[0-4].csv
   print "hadoop fs -get /data/tmp/ad4push/devices_android/full/$date/24/exportDevices_517_$date_with_zero"."_*.csv $base/\n";
   system("hadoop fs -get /data/tmp/ad4push/devices_android/full/$date/24/exportDevices_517_$date_with_zero"."_*.csv $base/");
   my $status = $?;

   # /data/tmp/ad4push/devices_ios/full/2015/09/02/24/exportDevices_515_20150902_[0-4].csv
   print "hadoop fs -get /data/tmp/ad4push/devices_ios/full/$date/24/exportDevices_515_$date_with_zero"."_*.csv $base/\n";
   system("hadoop fs -get /data/tmp/ad4push/devices_ios/full/$date/24/exportDevices_515_$date_with_zero"."_*.csv $base/");
   $status ||= $?;

   system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/push_devices_merge/ $base/*; bye\"");
   $status ||= $?;

   system("rm -rf /tmp/$date_with_zero");

   return $status;
}

sub upload_dcf_feed {
     my $base = "/tmp/$date_with_zero/dcf_feed/clickstream_merged_feed";
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
     system("rm -rf /tmp/$date_with_zero");
     return $status;
}

sub fetchFeedFile {
   my ($filename, $folderName, $base) = @_;

   # /data/tmp/variables/custWelcomeVoucher/daily/2015/09/26/20150927_CUST_WELCOME_VOUCHERS.csv
   print "hadoop fs -get /data/tmp/variables/$folderName/daily/$date/$filename $base/\n";

   # /data/tmp/variables/custWelcomeVoucher/daily/2015/09/26/20150927_CUST_WELCOME_VOUCHERS.csv
   system("hadoop fs -get /data/tmp/variables/$folderName/daily/$date/$filename $base/");
   my $status = $?;

   $status ||= removeNull("$base/$filename");

   return $status;
}


sub upload_email_campaigns_decryptFeedFiles {
    my $base = "/tmp/$date_with_zero/decryptFeedFiles";
    print "directory is $base\n";
    system("mkdir -p $base");
    my $status = $?;

    # 20150928_CONTACTS_LIST.csv
    my $filename = "$date_with_zero_today"."_CONTACTS_LIST.csv";
    my $folderName = "contactListMobile";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    $status ||= removeQuotes("$base/$filename");

    #print("rename file $base/$filename to dap_$filename");
    #$status ||= system("mv $base/$filename $base/dap_$filename");

    # 20150928_Contact_list_Plus.csv
    $filename = "$date_with_zero_today"."_Contact_list_Plus.csv";
    $folderName = "Contact_list_Plus";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    $status ||= removeQuotes("$base/$filename");

    #print("rename file $base/$filename to dap_$filename");
    #$status ||= system("mv $base/$filename $base/dap_$filename");

    system("lftp -c \"open -u cfactory,cF\@ct0ry 54.254.101.71 ;  mput -O /responsysfrom/ $base/*.csv; bye\"");
    $status ||= $?;
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/*; bye\"");
    $status ||= $?;
    system("rm -rf /tmp/$date_with_zero");
    return $status;
}


sub upload_email_campaigns_feedFiles {
    my $base = "/tmp/$date_with_zero/feedFiles";
    print "directory is $base\n";
    system("mkdir -p $base");
    my $status = $?;

    # 20150927_CUST_WELCOME_VOUCHERS.csv
    my $filename = "$date_with_zero_today"."_CUST_WELCOME_VOUCHERS.csv";
    my $folderName = "custWelcomeVoucher";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_CUST_PREFERENCE.csv
    $filename = "$date_with_zero_today"."_CUST_PREFERENCE.csv";
    $folderName = "custPreference";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_Customer_PREFERRED_TIMESLOT_part1.csv
    $filename = "$date_with_zero_today"."_Customer_PREFERRED_TIMESLOT_part1.csv";
    $folderName = "customerPreferredTimeslotPart1";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_Customer_PREFERRED_TIMESLOT_part2.csv
    $filename = "$date_with_zero_today"."_Customer_PREFERRED_TIMESLOT_part2.csv";
    $folderName = "customerPreferredTimeslotPart2";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_payback_data.csv
    $filename = "$date_with_zero_today"."_payback_data.csv";
    $folderName = "paybackData";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20151109_CUST_TOP5.csv
    $filename = "$date_with_zero_today"."_CUST_TOP5.csv";
    $folderName = "custTop5";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20151109_CUST_CAT_PURCH_PRICE.csv
    $filename = "$date_with_zero_today"."_CUST_CAT_PURCH_PRICE.csv";
    $folderName = "cat_avg";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20151109_CUST_CAT_PURCH_COUNT.csv
    $filename = "$date_with_zero_today"."_CUST_CAT_PURCH_COUNT.csv";
    $folderName = "cat_count";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150928_CUST_ORDERS.csv
    $filename = "$date_with_zero_today"."_CUST_ORDERS.csv";
    $folderName = "customerOrders";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150928_NL_data_list.csv
    $filename = "$date_with_zero_today"."_NL_data_list.csv";
    $folderName = "NL_data_list";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150928_app_email_feed.csv
    $filename = "$date_with_zero_today"."_app_email_feed.csv";
    $folderName = "app_email_feed";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_CUST_EMAIL_RESPONSE.csv
    $filename = "$date_with_zero_today"."_CUST_EMAIL_RESPONSE.csv";
    $folderName = "custEmailResponse";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    # 20150927_Customer_App_details.csv
    $filename = "$date_with_zero_today"."_Customer_App_details.csv";
    $folderName = "customerAppDetails";
    $status ||= fetchFeedFile($filename, $folderName, $base);

    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i /data/responsys/u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mput -O upload/ $base/*;'");
    $status ||= $?;

    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/*; bye\"");
    $status ||= $?;

    system("rm -rf /tmp/$date_with_zero");
    return $status;
}

sub upload_email_campaigns {
    my $base = "/tmp/$date_with_zero/email_campaigns";

    print "email campaigns directory is $base\n";
    system("mkdir -p $base");

    my $filename = "$date_with_zero_today"."_LIVE_CAMPAIGN.csv";
    my $filenameZip = "$date_with_zero_today"."_LIVE_CAMPAIGN.zip";


    my $followUp_filename = "$date_with_zero_today"."_live_campaign_followup.csv";

    print "hadoop fs -get /data/tmp/email_campaigns/merged/daily/$date/$filename $base/\n";

    system("hadoop fs -get /data/tmp/email_campaigns/merged/daily/$date/$filename $base/");
    my $status = $?;

    print "hadoop fs -get /data/tmp/email_campaigns/follow_up_campaigns/daily/$date/$followUp_filename $base/\n";

    system("hadoop fs -get /data/tmp/email_campaigns/follow_up_campaigns/daily/$date/$followUp_filename $base/");

    $status ||= removeNull("$base/$filename");

    $status ||= removeNull("$base/$followUp_filename");

    print("zip $base/$filenameZip $base/$filename\n");
    system("zip $base/$filenameZip $base/$filename");

    print("rm $base/$filename");
    system("rm $base/$filename");

    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i /data/responsys/u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mput -O upload/ $base/*;'");
    $status ||= $?;

    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $base/* ; bye\"");
    $status ||= $?;

    system("rm -rf /tmp/$date_with_zero");
    return $status;
}

sub upload_customer_device_mapping_feed {
    my $base = "/tmp/$date_with_zero/extras";

    print "extras directory is $base\n";
    system("mkdir -p $base");

    my $filename = "$date_with_zero_today"."_CUSTOMER_DEVICE_MAPPING_FEED.csv";
    my $filenameZip = "$date_with_zero_today"."_CUSTOMER_DEVICE_MAPPING_FEED.zip";

    print "hadoop fs -get /data/tmp/extras/device_mapping/full/$date/24/$filename $base/\n";
    system("hadoop fs -get /data/tmp/extras/device_mapping/full/$date/24/$filename $base/");

    my $status = $?;

    print("zip $base/$filenameZip $base/$filename\n");
    system("zip $base/$filenameZip $base/$filename");

    print("rm $base/$filename");
    system("rm $base/$filename");

    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i /data/responsys/u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mput -O upload/ $base/*;'");
    $status ||= $?;

    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/extras/ $base/* ; bye\"");
    $status ||= $?;

    system("rm -rf /tmp/$date_with_zero");
    return $status;
}

sub upload_calendar_replenish_campaigns {
    my $calendar_base = "/tmp/$date_with_zero/calendar_campaigns";

    print "calendar campaigns directory is $calendar_base\n";
    system("mkdir -p $calendar_base");

    my $calendar_filename = "$date_with_zero_today"."_DCF_CAMPAIGN.csv";
    my $calendar_filenameZip = "$date_with_zero_today"."_DCF_CAMPAIGN.zip";

    my $replenish_filename = "$date_with_zero_today"."_replenishment.csv";

    print "hadoop fs -get /data/tmp/calendar_campaigns/merged/daily/$date/$calendar_filename $calendar_base/\n";
    system("hadoop fs -get /data/tmp/calendar_campaigns/merged/daily/$date/$calendar_filename $calendar_base/");
    my $calendar_status = $?;

    print "hadoop fs -get /data/tmp/calendar_campaigns/replenishment/daily/$date/$replenish_filename $calendar_base/\n";
    system("hadoop fs -get /data/tmp/calendar_campaigns/replenishment/daily/$date/$replenish_filename $calendar_base/");

    $calendar_status ||= removeNull("$calendar_base/$calendar_filename");
    $calendar_status ||= removeNull("$calendar_base/$replenish_filename");

    print("zip $calendar_base/$calendar_filenameZip $calendar_base/$calendar_filename\n");
    system("zip $calendar_base/$calendar_filenameZip $calendar_base/$calendar_filename");

    print("rm $calendar_base/$calendar_filename");
    system("rm $calendar_base/$calendar_filename");

    #upload both files to dapshare
    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $calendar_base/* ; bye\"");
    $calendar_status ||= $?;

    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i /data/responsys/u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mput -O upload/ $calendar_base/$calendar_filenameZip;'");
    $calendar_status ||= $?;

    #rename and upload Replenish file for decryption of email
    # print("rename file $calendar_base/$replenish_filename to dap_$replenish_filename");
    # $calendar_status ||= system("mv $calendar_base/$replenish_filename $calendar_base/dap_$replenish_filename");
    # system("lftp -c \"open -u cfactory,cF\@ct0ry 54.254.101.71 ;  mput -O /responsysfrom/ $calendar_base/dap_$replenish_filename; bye\"");
    system("lftp -c \"open -u cfactory,cF\@ct0ry 54.254.101.71 ;  mput -O /responsysfrom/ $calendar_base/$replenish_filename; bye\"");
    $calendar_status ||= $?;

    system("rm -rf /tmp/$date_with_zero");
    return $calendar_status;
}


sub upload_acart_hourly_campaign {
    my $acart_hourly_base = "/tmp/$date_with_zero/campaigns/acart_hourly";

    print "acart campaigns directory is $acart_hourly_base\n";

    system("mkdir -p $acart_hourly_base");

    print "acart hourly campaigns directory is $acart_hourly_base\n";

    my $acart_hourly_filename = "$date_with_zero_today"."_$current_hour"."_LIVE_ACART_HOURLY.csv";

    print "hadoop fs -get /data/tmp/email_campaigns/acart_hourly/hourly/$date_today/$current_hour/$acart_hourly_filename $acart_hourly_base/\n";

    system("hadoop fs -get /data/tmp/email_campaigns/acart_hourly/hourly/$date_today/$current_hour/$acart_hourly_filename $acart_hourly_base/");
    my $acart_hourly_status = $?;

    $acart_hourly_status ||= removeNull("$acart_hourly_base/$acart_hourly_filename");

    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i /data/responsys/u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mput -O upload/ $acart_hourly_base/*;'");
    $acart_hourly_status ||= $?;

    system("lftp -c \"open -u dapshare,dapshare\@12345 54.254.101.71 ;  mput -O crm/email_campaigns/ $acart_hourly_base/* ; bye\"");
    $acart_hourly_status ||= $?;

    system("rm -rf /tmp/$date_with_zero");
    return $acart_hourly_status;

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
    my $base = "/tmp/$date_with_zero/pricing_sku_data";
    print "pricing sku data directory is $base\n";
    system("mkdir -p $base");

   # /data/tmp/sku_data/pricing/daily/2015/08/19/part-00000
   print "hadoop fs -get /data/tmp/sku_data/pricing/daily/$date/sku_data_pricing_$date_with_zero.csv $base/\n";

   # /data/tmp/sku_data/pricing/daily/2015/08/19/part-00000
   system("hadoop fs -get /data/tmp/sku_data/pricing/daily/$date/sku_data_pricing_$date_with_zero.csv $base/");
   my $status = $?;
   # gzipping the file
   system("gzip -c /tmp/$date_with_zero/pricing_sku_data/sku_data_pricing_$date_with_zero.csv >>/tmp/$date_with_zero/pricing_sku_data/$date_with_zero.gz");
   $status ||= $?;
   # encrypting
   system("gpg --batch -c --passphrase kJFdvnkl\@25293kD\$gj -o /tmp/$date_with_zero/pricing_sku_data/$date_with_zero /tmp/$date_with_zero/pricing_sku_data/$date_with_zero.gz");
   $status ||= $?;
   # copying to slave location
   system("scp /tmp/$date_with_zero/pricing_sku_data/$date_with_zero 172.16.84.192:/var/www/html/data/sku-pageview-summary/$date_with_zero");
   $status ||= $?;
   system("rm -rf /tmp/$date_with_zero");
   return $status;
}

#this method will remove double quiets from header and remove null from content
sub removeNull {

    #read input file path
    my ($inputFile) = @_;

    #rename file
    system("mv $inputFile $inputFile._old");
    my $status = $?;

    #remove double quotes and null from content
    system("cat $inputFile._old | sed -e 's/\"\"//g' | sed -e 's/\";\"/;/g' | sed -e 's/^\"//g' | sed -e 's/\"\$//g' | sed -e 's/\;null;/;;/g' | sed -e 's/^null;/;/g' | sed -e 's/\;null\$/;/g' > $inputFile");

    $status ||= $?;

    #remove old file
    system("rm $inputFile._old");
    $status ||= $?;

    return $status;
}


sub removeQuotes {
    my ($inputFile) = @_;

    system("mv $inputFile $inputFile._old");
    my $status = $?;

    system("cat $inputFile._old | sed -e 's/\"#/#/g' | sed -e 's/#\"/#/g' > $inputFile");

    $status ||= $?;

    system("rm $inputFile._old");
    $status ||= $?;

    return $status;

}
