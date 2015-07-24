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
    
    send_mail($subject, $msg);
}

# base params
my $HDFS_BASE;
my $EMAIL_PREFIX;

# target needs to be either stage or prod
if ($target eq "stage") {
    $HDFS_BASE = "hdfs://bigdata-master.jabong.com:8020";
    $EMAIL_PREFIX = "[STAGE]";
} elsif ($target eq "prod") {
    $HDFS_BASE = "hdfs://dataplatform-master.jabong.com:8020";
    $EMAIL_PREFIX = "[PROD]";
} else {
    print "not a valid target\n";
    exit -1;
}

# spark path constants
my $BASE_PATH = "/opt/alchemy-core/current";
my $SPARK_HOME = "/ext/spark";
my $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster ";
my $HIVE_JARS = "--jars /ext/spark/lib/datanucleus-api-jdo-3.2.6.jar,/ext/spark/lib/datanucleus-core-3.2.10.jar,/ext/spark/lib/datanucleus-rdbms-3.2.9.jar --files /ext/spark/conf/hive-site.xml";
my $DRIVER_CLASS_PATH = "--driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar ";
my $CORE_JAR = "$BASE_PATH/jar/Alchemy-assembly.jar";
my $HDFS_CONF = "$HDFS_BASE/apps/alchemy/conf";
my $AMMUNITION = "--num-executors 3 --executor-memory 9G";

# bobAcq & merge
if ($component eq "bob") {
    my $command1 = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull1.json";
    run_component($component, $command1);
    my $command2 = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqIncr.json";
    run_component($component, $command2);
    my $command3 = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/bobMerge.json";
    run_component($component, $command3);
# bob acq run for only customer_product_shortlist full dump separately as this takes a lot of time.
} elsif ($component eq "bobFull") {
    my $command = "$BASE_SPARK_SUBMIT $DRIVER_CLASS_PATH --num-executors 3 --executor-memory 27G $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcqFull2.json";
    run_component($component, $command);
# erpAcq & merge
} elsif ($component eq "erp") {
    my $command1 = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/erpAcqIncr.json";
    run_component($component, $command1);
    my $command2 = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component merge --config $HDFS_CONF/config.json --mergeJson $HDFS_CONF/erpMerge.json";
    run_component($component, $command2);
} elsif ($component eq "retargetPushCampaign") {
    # for retarget campaign module
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component pushRetargetCampaign --config $HDFS_CONF/config.json";
    run_component($component, $command);
} elsif ($component eq "basicItr") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $HIVE_JARS $CORE_JAR --component basicItr --config $HDFS_CONF/config.json";
    run_component($component, $command);
} else {
   print "not a valid component\n";
}


sub send_mail {
    my ($subject, $msg) = @_;
    sendmail(
        From    => 'tech.dap@jabong.com',
        To      => 'tech.dap@jabong.com',
        Subject => "$EMAIL_PREFIX $subject ",
        Message => $msg,
    );
}
