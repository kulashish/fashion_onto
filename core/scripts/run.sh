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

GetOptions(
    'target|t=s' => \$target,
    'component|c=s' => \$component,
    'debug|d' => \$debug,
) or die "Usage: $0 --debug  --target|-t stage|prod --component|-c NAME\n";
 

# 
sub run_component {
    my ($component, $command) = @_;
    my $start = time();
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
my $SPARK_HOME = "/ext/spark";
my $BASE_SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit --class \"com.jabong.dap.init.Init\" --master yarn-cluster --driver-class-path /usr/share/java/mysql-connector-java-5.1.17.jar ";
my $CORE_JAR = "/opt/alchemy-core/current/jar/Alchemy-assembly-0.1.jar";
my $HDFS_CONF = "$HDFS_BASE/apps/alchemy/conf";
my $AMMUNITION = "--num-executors 3 --executor-memory 9G";

# bobAcq
if ($component eq "bobAcq") {
    my $command = "$BASE_SPARK_SUBMIT $AMMUNITION $CORE_JAR --component acquisition --config $HDFS_CONF/config.json --tablesJson $HDFS_CONF/bobAcquisitionTables.json";
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
