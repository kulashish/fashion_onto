#!/usr/bin/env perl

use strict;
use 5.010;
use warnings;
use Getopt::Long qw(GetOptions);
Getopt::Long::Configure qw(gnu_getopt);
use Data::Dumper;
use Time::HiRes qw( time );

my $folderName;
my $fileName;
my $status;

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time() - 60*60*24);

my $date_with_zero = strftime "%Y%m%d", localtime(time());

my $base = "/tmp/decrypt/$date_with_zero";
print "folderName is $base\n";
system("mkdir -p $base");

exit download_campaigns();

sub fetchAndRenameFile {
   my ($fileName, $folderName, $base) = @_;

   print "hadoop fs -get /data/test/tmp/variables/$folderName/daily/$date/$fileName $base/\n";
   system("hadoop fs -get /data/test/tmp/variables/$folderName/daily/$date/$fileName $base/");

   my $status = $?;

   $status ||= removeNull("$base/$fileName");

   # renaming the file for test
   print("rename file $base/$fileName.csv to dap_$fileName.csv");
   $status ||= system("mv $base/$fileName.csv $base/dap_$fileName.csv");

   return $status;
}

sub download_campaigns {
    # 20150928_CONTACTS_LIST.csv
    $fileName = "$date_with_zero\_CONTACTS_LIST.csv";
    $folderName = "contactListMobile";

    $status ||= fetchAndRenameFile($fileName, $folderName, $base);

    $fileName = "$date_with_zero\_Contact_list_Plus.csv";
    $folderName = "Contact_list_Plus";

    $status ||= fetchAndRenameFile($fileName, $folderName, $base);

    system("lftp -c \"open -u cfactory,cF\@ct0ry 54.254.101.71 ;  mput -O /responsysfrom/ $base/*.csv; bye\"");
    $status ||= $?;
    system("rm -rf /tmp/decrypt");
    return $status;
}
