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
my $target;
my $component;

GetOptions (
    'target|t=s' => \$target,
    'component|c=s' => \$component,
    'debug|d' => \$debug,
) or die "Usage: $0 --debug --target|-t STAGE|PROD|TEST-PROD|DEV-PROD --component|-c <component name>\n";
 

# base params
my $HDFS_BASE;
my $EMAIL_PREFIX;
my $HDFS_LIB;
my $HDFS_CONF;

# 
sub run_component {
    my ($component, $command) = @_;
    my $start = time();

    # set the perl environment variable YARN_CONF_DIR
    $ENV{'YARN_CONF_DIR'} = '/etc/hadoop/conf';
    print $ENV{'YARN_CONF_DIR'}."\n";

    system($command);

    my $status = $?;
    my $end = time();

    my $statusStr = "";
    if ($status == -1) {
        $statusStr =  "failed to execute: $!\n";
    }
    elsif ($status & 127) {
        $statusStr =  sprintf("child died with signal %d, %s coredump\n", ($status & 127),  ($status & 128) ? 'with' : 'without');
    }
    else {
        $statusStr =  sprintf("child exited with value %d\n", $? >> 8);
    }
    my $job_status = "";

    if($status != 0){
        $job_status = "[FAILED]"
    }

    my $diff = $end - $start;

    my $msg = "\n";
    $msg .= "Command: $command\n";
    $msg .= sprintf("Time Taken: %.2f secs\n",$diff);
    $msg .= "start: " . localtime($start) . "\n";
    $msg .= "end: " . localtime($end) . "\n";
    $msg .= "Status: " . $statusStr . "\n";

    my $subject = "run of $component @ ". localtime($start);
    print "$subject\n\n";
    print "$msg\n\n";

    if($EMAIL_PREFIX ne "[DEV]"){
        send_mail($job_status, $subject, $msg);
    }

    return $status;
}

# spark path constants
my $SPARK_HOME = "/ext/spark";
my $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
my $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
my $DRIVER_CLASS_PATH = "--driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar";
my $AMMUNITION = "--num-executors 27 --executor-memory 1G";

# target needs to be either stage or prod
if ($target eq "STAGE") {
    $HDFS_BASE = "hdfs://bigdata-master.jabong.com:8020";
    $HDFS_LIB = "$HDFS_BASE/apps/alchemy/workflows/lib";
    $HDFS_CONF = "$HDFS_BASE/apps/alchemy/conf";
    $EMAIL_PREFIX = "[STAGE]";
} elsif ($target eq "PROD") {
    $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
    $HDFS_LIB = "$HDFS_BASE/apps/alchemy/workflows/lib";
    $HDFS_CONF = "$HDFS_BASE/apps/alchemy/conf";
    $EMAIL_PREFIX = "[PROD]";
} elsif ($target eq "TEST-PROD") {
    $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
    $HDFS_LIB = "$HDFS_BASE/apps/test/alchemy/workflows/lib";
    $HDFS_CONF = "$HDFS_BASE/apps/test/alchemy/conf";
    $EMAIL_PREFIX = "[TEST-PROD]";
} else {

    my $hostname =  `hostname`;
    chomp($hostname);

    my $USER_NAME = `whoami`;
    chomp($USER_NAME);

    if ($hostname =~ /^bigdata/) {
        $HDFS_BASE = "hdfs://bigdata-master.jabong.com:8020";
    } elsif ($hostname =~ /^dataplatform/) {
        $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
    } else {
        print("Error: not supported platform");
        exit(-1);
    }

    if (exists $ENV{"ALCHEMY_CORE_HOME"}) {
      $HDFS_LIB = $ENV{"ALCHEMY_CORE_HOME"} . "/jar";
    } else {
     $HDFS_LIB = "/home/$USER_NAME/alchemy/current/jar";
    }

    $HDFS_CONF = "$HDFS_BASE/user/$USER_NAME/alchemy/conf";
    $EMAIL_PREFIX = "[DEV]";
}

my $CORE_JAR = "$HDFS_LIB/Alchemy-assembly.jar";
my $job_exit;

# for bob Acq of first set of full tables
if ($component eq "bobAcqFull1") {
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull1.json";
    $job_exit = run_component($component, $command);
# bob acq run for only customer_product_shortlist full dump separately as this takes a lot of time.
} elsif ($component eq "bobAcqFull2") {
    $AMMUNITION = "--num-executors 3 --executor-memory 27G";
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull2.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "bobAcqIncr") {
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqIncr.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "bobAcqHourly") {
    $AMMUNITION = "--num-executors 1 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqHourly.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "bobMerge") {
    $AMMUNITION = "--num-executors 12 --executor-memory 18G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/bobMerge.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "bobMergeMonthly") {
    $AMMUNITION = "--num-executors 27 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/bobMergeMonthly.json";
    $job_exit = run_component($component, $command);
# erp Acquisition
} elsif ($component eq "erpAcqIncr") {
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/erpAcqIncr.json";
    $job_exit = run_component($component, $command);
#erp Merge
} elsif ($component eq "erpMerge") {
    $AMMUNITION = "--num-executors 9 --executor-memory 18G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/erpMerge.json";
    $job_exit = run_component($component, $command);
# crm acquisition
} elsif ($component eq "crmAcqIncr") {
    $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
    $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
    $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
    $AMMUNITION = "--num-executors 3 --executor-memory 18G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/crmAcqIncr195.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "crmAcqFull") {
    $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
    $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
    $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/crmAcqFull195.json";
    $job_exit = run_component($component, $command);
#crm Merge
} elsif ($component eq "crmMerge") {
    $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
    $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
    $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
    $AMMUNITION = "--num-executors 9 --executor-memory 18G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/crmMerge195.json";
    $job_exit = run_component($component, $command);
#unicommerce acquisition
} elsif($component eq "unicommerceAcqIncr") {
   $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
   $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
   $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
   $AMMUNITION = "--num-executors 3 --executor-memory 18G";
   my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/unicommerceAcqIncr.json";
   $job_exit = run_component($component, $command);
} elsif($component eq "unicommerceMerge") {
   $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
   $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
   $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
   $AMMUNITION = "--num-executors 9 --executor-memory 18G";
   my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/unicommerceMerge.json";
   $job_exit = run_component($component, $command);
#responsys files merger
} elsif ($component eq "dndMerger") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component dndMerger --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/dndMerger.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "smsOptOutMerger") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component smsOptOutMerger --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/smsOptOutMerger.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "basicITR") {
    $AMMUNITION = "--num-executors 10 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component basicITR --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/basicITR.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "recommendations") {
    $AMMUNITION = "--num-executors 10 --executor-memory 500M";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component recommendations --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/recommendation.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "clickstreamYesterdaySession") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamYesterdaySession --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamYesterdaySession.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "clickstreamSurf3Variable") {
    $AMMUNITION = "--num-executors 5 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamSurf3Variable --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamSurf3Variable.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerDeviceMapping") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION --driver-memory 9G $CORE_JAR --component customerDeviceMapping --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerDeviceMapping.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "surfCampaigns") {
    $AMMUNITION = "--num-executors 15 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component surfCampaigns --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "retargetCampaigns") {
    # for retarget campaign module
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component retargetCampaigns --config $HDFS_CONF/config.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "invalidCampaigns") {
    $AMMUNITION = "--num-executors 15 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component invalidCampaigns --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "abandonedCartCampaigns") {
    $AMMUNITION = "--num-executors 15 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component abandonedCartCampaigns --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "wishlistCampaigns") {
    $AMMUNITION = "--num-executors 15 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component wishlistCampaigns --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "miscellaneousCampaigns") {
    $AMMUNITION = "--num-executors 7 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component miscellaneousCampaigns --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/emailCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "followUpCampaigns") {
    $AMMUNITION = "--num-executors 8 --executor-memory 1G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component followUpCampaigns --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/followUpCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "loveCalendarCampaigns") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component loveCalendarCampaigns --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/loveCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "acartHourly") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acartHourly --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/acartHourly.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "pushCampaignMerge") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushCampaignMerge --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "emailCampaignMerge") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component emailCampaignMerge --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/emailCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "emailCampaignMergeFeed") {
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component emailCampaignMergeFeed --config $HDFS_CONF/config.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "calendarCampaignMerge") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component calendarCampaignMerge --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/calendarCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "calendarCampaignMergeFeed") {
    $AMMUNITION = "--num-executors 3 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component calendarCampaignMergeFeed --config $HDFS_CONF/config.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "mobilePushCampaignQuality") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component mobilePushCampaignQuality --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/pushCampaigns.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "emailCampaignQuality") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component emailCampaignQuality --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/emailCampaigns.json";
    $job_exit =run_component($component, $command);
} elsif ($component eq "calendarCampaignQuality") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component calendarCampaignQuality --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/calendarCampaigns.json";
    $job_exit =run_component($component, $command);
} elsif ($component eq "variablesQuality") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component variablesQuality --config $HDFS_CONF/config.json --campaignsJson $HDFS_CONF/variablesQuality.json";
    $job_exit =run_component($component, $command);
} elsif ($component eq "campaignQuality") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component campaignQuality --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/campaignQuality.json";
    $job_exit = run_component($component, $command);
# ad4push
} elsif ($component eq "ad4pushCustomerResponse") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component ad4pushCustomerResponse --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/ad4pushCustomerResponse.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "ad4pushDeviceMerger") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component ad4pushDeviceMerger --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/ad4pushDeviceMerger.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "pricepointCampaign") {
    $AMMUNITION = "--num-executors 7 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pricepointCampaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/pricepointCampaign.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerSurfAffinity") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component customerSurfAffinity --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerSurfAffinity.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "hottestXCampaign") {
    $AMMUNITION = "--num-executors 7 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component hottestXCampaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/hottestXCampaign.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "pricingSKUData") {
    $AMMUNITION = "--num-executors 9 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pricingSKUData --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/pricingSKUData.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "replenishmentCampaign") {
    $AMMUNITION = "--num-executors 25 --executor-memory 2G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component replenishmentCampaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/replenishmentCampaign.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "replenishmentCampaignFeed") {
    $AMMUNITION = "--num-executors 15 --executor-memory 2G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component replenishmentCampaignFeed --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/replenishmentCampaignFeed.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "brandInCityCampaign") {
    $AMMUNITION = "--num-executors 25 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component brandInCityCampaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/brandInCityCampaign.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "brickAffinityCampaign") {
    $AMMUNITION = "--num-executors 27 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component brickAffinityCampaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/brickAffinityCampaign.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "geoCampaign") {
    $AMMUNITION = "--num-executors 9 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component geo_campaign --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/geoCampaign.json";
    $job_exit = run_component($component, $command);  
} elsif ($component eq "clearanceCampaign") {
     $AMMUNITION = "--num-executors 9 --executor-memory 3G";
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clearance --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clearanceCampaign.json";
     $job_exit = run_component($component, $command);
} elsif ($component eq "pricingSKUData") {
    $AMMUNITION = "--num-executors 9 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pricingSKUData --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/pricingSKUData.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "dcfFeedGenerate") {
    $AMMUNITION = "--num-executors 15 --executor-memory 2G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION  $HIVE_JARS $CORE_JAR --component dcfFeedGenerate --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/dcfFeedGenerate.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "clickstreamDataQualityCheck") {
    $AMMUNITION = "--num-executors 9 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamDataQualityCheck --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamDataQualityCheck.json";
    $job_exit = run_component($component, $command);
# Data Feeds
} elsif ($component eq "custPreference") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component custPreference --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/custPreference.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "custWelcomeVoucher") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component custWelcomeVoucher --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/custWelcomeVoucher.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "custTop5") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component custTop5 --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/custTop5.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "salesOrderAddrFav") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component salesOrderAddrFav --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/salesOrderAddrFav.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "salesItemRevenue") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component salesItemRevenue --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/salesItemRevenue.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerOrders") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component customerOrders --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerOrders.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "contactListMobile") {
    $AMMUNITION = "--num-executors 18 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component contactListMobile --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/contactListMobile.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerPreferredTimeslotPart2") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component customerPreferredTimeslotPart2 --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerPreferredTimeslotPart2.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerPreferredTimeslotPart1") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component customerPreferredTimeslotPart1 --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerPreferredTimeslotPart1.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "paybackData") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component paybackData --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/paybackData.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerAppDetails") {
    $AMMUNITION = "--num-executors 10 --executor-memory 4G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component customerAppDetails --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerAppDetails.json";
    $job_exit = run_component($component, $command);
} elsif($component eq "custEmailResponse") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component custEmailResponse --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/custEmailResponse.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "cityWiseData") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component cityWiseData --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/cityWiseData.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "acartHourlyFeed") {
    my $AMMUNITION = "--num-executors 5 --executor-memory 400M";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component acartHourlyFeed --config $HDFS_CONF/config.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "customerMasterRecordFeed") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component customerMasterRecordFeed --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/customerMasterRecordFeed.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "shopTheLook") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component shopTheLook --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/shopTheLook.json";
    $job_exit = run_component($component, $command);
} elsif ($component eq "winbackCustomer") {
    $SPARK_HOME = "/ext/spark-1.5.1-bin-hadoop2.6";
    $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --name $component";
    $HIVE_JARS = "--jars $SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar --files $SPARK_HOME/conf/hive-site.xml";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component winbackCustomer --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/winbackCustomer.json";
    $job_exit = run_component($component, $command);
} else {
    print "not a valid component\n";
    $job_exit = -1;
}

exit $job_exit;


sub send_mail {
    my ($job_status, $subject, $msg) = @_;
    sendmail(
        From    => 'tech.dap@jabong.com',
        To      => 'tech.dap@jabong.com',
        Subject => "$job_status $EMAIL_PREFIX $subject ",
        Message => $msg,
    );
}
