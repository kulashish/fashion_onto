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
) or die "Usage: $0 --debug  --target|-t stage|prod --component|-c NAME\n";
 

# 
sub run_component {
    my ($component, $command) = @_;
    my $start = time();
    my $YARN_CONF_DIR = "YARN_CONF_DIR=/etc/hadoop/conf ";
    system($YARN_CONF_DIR . $command);
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
    $msg .= "Command: $YARN_CONF_DIR $command\n";
    $msg .= sprintf("Time Taken: %.2f secs\n",$diff);
    $msg .= "start: " . localtime($start) . "\n";
    $msg .= "end: " . localtime($end) . "\n";
    $msg .= "Status: " . $statusStr . "\n";

    my $subject = "run of $component @ ". localtime($start);
    print "$subject\n\n";
    print "$msg\n\n";

    send_mail($job_status, $subject, $msg);
}

# base params
my $HDFS_BASE;
my $EMAIL_PREFIX;

# target needs to be either stage or prod
if ($target eq "STAGE") {
    $HDFS_BASE = "hdfs://bigdata-master.jabong.com:8020";
    $EMAIL_PREFIX = "[STAGE]";
} elsif ($target eq "PROD") {
    $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
    $EMAIL_PREFIX = "[PROD]";
} elsif ($target eq "TEST-PROD") {
     $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
     $EMAIL_PREFIX = "[TEST-PROD]";
}else {
    print "not a valid target\n";
    exit -1;
}

# spark path constants
my $SPARK_HOME = "/ext/spark";
my $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster ";
my $HIVE_JARS = "--jars /ext/spark/lib/datanucleus-api-jdo-3.2.6.jar,/ext/spark/lib/datanucleus-core-3.2.10.jar,/ext/spark/lib/datanucleus-rdbms-3.2.9.jar --files /ext/spark/conf/hive-site.xml";
my $DRIVER_CLASS_PATH = "--driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar ";
my $HDFS_LIB = "$HDFS_BASE/apps/alchemy/workflows/lib";
my $CORE_JAR = "$HDFS_LIB/Alchemy-assembly.jar";
my $HDFS_CONF = "$HDFS_BASE/apps/alchemy/conf";
my $AMMUNITION = "--num-executors 3 --executor-memory 9G";

# for bob Acq of first set of full tables
if ($component eq "bobAcqFull1") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull1.json";
    run_component("bob Acquisition for Full tables", $command);
# bob acq run for only customer_product_shortlist full dump separately as this takes a lot of time.
} elsif ($component eq "bobAcqFull2") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH --num-executors 3 --executor-memory 27G $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull2.json";
    run_component("bob Acquisition for customer_product_shortlist table", $command);
} elsif ($component eq "bobAcqIncr") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqIncr.json";
    run_component("bob Acquisition for Incremental tables", $command);
} elsif ($component eq "bobMerge") {
    $AMMUNITION = "--num-executors 27 --executor-memory 3G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/bobMerge.json";
    run_component("bob Merge for Incremental tables", $command);
# erp Acquisition
} elsif ($component eq "erpAcqIncr") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/erpAcqIncr.json";
    run_component("erp Acquisition for Incremental tables", $command);
#erp Merge
} elsif ($component eq "erpMerge") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/erpMerge.json";
    run_component("erp Merge for Incremental tables", $command);
} elsif ($component eq "pushRetargetCampaign") {
    # for retarget campaign module
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component pushRetargetCampaign --config $HDFS_CONF/config.json";
    run_component($component, $command);
} elsif ($component eq "clickstreamYesterdaySession") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamYesterdaySession --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamYesterdaySession.json";
    run_component($component, $command);
} elsif ($component eq "clickstreamSurf3Variable") {
    $AMMUNITION = "--num-executors 5 --executor-memory 9G";
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamSurf3Variable --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamSurf3Variable.json";
    run_component($component, $command);
} elsif ($component eq "basicItr") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component basicItr --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/basicITR.json";
    run_component($component, $command);
} elsif ($component eq "pushInvalidCampaign") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushInvalidCampaign --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
    run_component($component, $command);
} elsif ($component eq "pushAbandonedCartCampaign") {
     $AMMUNITION = "--num-executors 10 --executor-memory 6G";
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushAbandonedCartCampaign --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
     run_component($component, $command);
} elsif ($component eq "pushWishlistCampaign") {
     $AMMUNITION = "--num-executors 10 --executor-memory 6G";
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushWishlistCampaign --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
     run_component($component, $command);
} elsif ($component eq "pushCampaignMerge") {
     $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster ";
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushCampaignMerge --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
     run_component($component, $command);
} elsif ($component eq "deviceMapping") {
       my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component deviceMapping --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/deviceMapping.json";
       run_component($component, $command);
} elsif ($component eq "Ad4pushCustReact") {
       my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component Ad4pushCustReact --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/ad4pushCustomerResponse.json";
       run_component($component, $command);
} elsif ($component eq "Ad4pushDeviceMerger") {
       my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component Ad4pushDeviceMerger --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/ad4pushDeviceMerger.json";
       run_component($component, $command);
} elsif ($component eq "pushSurfCampaign") {
    $AMMUNITION = "--num-executors 7 --executor-memory 9G";
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pushSurfCampaign --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
     run_component($component, $command);
} elsif ($component eq "pricingSKUData") {
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component pricingSKUData --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/pricingSKUData.json";
          run_component($component, $command);
} elsif ($component eq "mobilePushCampaignQuality") {
     my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component mobilePushCampaignQuality --config $HDFS_CONF/config.json --pushCampaignsJson $HDFS_CONF/pushCampaigns.json";
     run_component($component, $command);
} elsif ($component eq "dcfFeedGenerate") {
       my $command = "$BASE_SPARK_SUBMIT $AMMUNITION  $HIVE_JARS $CORE_JAR --component dcfFeedGenerate --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/dcfFeedGen.json";
       run_component($component, $command);
} elsif ($component eq "campaignQuality") {
     my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component campaignQuality --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/campaignQuality.json";
     run_component($component, $command);
} elsif ($component eq "clickstreamDataQualityCheck") {
      my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component clickstreamDataQualityCheck --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/clickstreamDataQualityCheck.json";
      run_component($component, $command);
} elsif ($component eq "dndMerger") {
      my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component dndMerger --config $HDFS_CONF/config.json --paramJson $HDFS_CONF/dndMerger.json";
      run_component($component, $command);
} else {
      print "not a valid component\n";
}


sub send_mail {
    my ($job_status, $subject, $msg) = @_;
    sendmail(
        From    => 'tech.dap@jabong.com',
        To      => 'tech.dap@jabong.com',
        Subject => "$job_status $EMAIL_PREFIX $subject ",
        Message => $msg,
    );
}
