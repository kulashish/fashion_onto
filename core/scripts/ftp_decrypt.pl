#!/usr/bin/env perl

use strict;
use 5.010;
use warnings;
use Getopt::Long qw(GetOptions);
Getopt::Long::Configure qw(gnu_getopt);
use Data::Dumper;
use Time::HiRes qw( time );

my $directory;
my $fileName;
GetOptions (
    'directory|d=s' => \$directory,
    'fileName|f=s' => \$fileName,
) or die "Usage: $0 --directory|-d -fileName|-f \n";

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);

my $date_with_zero = strftime "%Y%m%d", localtime(time());

my $base = "/tmp/decrypt/$date_with_zero";
print "directory is $base\n";
system("mkdir -p $base");
my $status = $?;

print "hadoop fs -get /data/tmp/variables/$directory/daily/$date/$date_with_zero\_$fileName.csv $base/\n";
system("hadoop fs -get /data/tmp/variables/$directory/daily/$date/$date_with_zero\_$fileName.csv $base/\n");

# renaming the file for test
print("rename file $base/$date_with_zero\_$fileName.csv to dap_$date_with_zero\_$fileName.csv");
system("mv $base/$date_with_zero\_$fileName.csv $base/dap_$date_with_zero\_$fileName.csv");

system("lftp -c \"open -u cfactory,cF\@ct0ry 54.254.101.71 ;  mput -O /responsysfrom/ $base/dap_$date_with_zero\_$fileName.csv; bye\"");

system("rm -rf /tmp/decrypt");

# if we wantt to download the  file
#        use Net::FTP;
#        $ftp = Net::FTP->new("some.host.name", Debug => 0)
#          or die "Cannot connect to some.host.name: $@";
#        $ftp->login("anonymous",'-anonymous@')
#          or die "Cannot login ", $ftp->message;
#        $ftp->cwd("/pub")
#          or die "Cannot change working directory ", $ftp->message;
#        $ftp->get("that.file")
#          or die "get failed ", $ftp->message;
#        $ftp->quit;