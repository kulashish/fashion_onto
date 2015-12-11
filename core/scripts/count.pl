#!/usr/bin/env perl

use POSIX qw(strftime);

my $date = strftime "%Y/%m/%d", localtime(time());
#print $date . "\n";

my $date_with_zero = strftime "%Y%m%d", localtime(time());
#print $date_with_zero . "\n";

my $date_with_zero_yesterday = strftime "%Y%m%d", localtime(time() - 60*60*24);
#print $date_with_zero_yesterday . "\n";

$date_with_zero = $date_with_zero_yesterday;


sub down_from_responsys {
    my ($feed) = @_;
    system("lftp -c 'set sftp:connect-program \"ssh -a -x -i ./u1.pem\"; connect sftp://jabong_scp:dummy\@files.dc2.responsys.net; mget archive/$feed.zip ;'");
    system("unzip $feed.zip");
}


my %data = (46,"cancel_retarget",47,"return_retarget",42,"acart_daily",
	 53,"wishlist_followup",45,"acart_iod",54,"wishlist_iod",44,"acart_lowstock",
	 55,"wishlist_lowstock",43,"acart_followup",57,"surf2",71,"surf6",
	 48,"invalid_followup",49,"invalid_lowstock",56,"surf1",58,"surf3",67,
	 "mipr",68,"new_arrivals_brand",52,"wishlist_reminder",100,"invalid_iod",
	 41,"acart_hourly",
	 24,"pricepoint",400,"beauty_campaign",500,"non_beauty_campaign",24,
	 "brand_in_city",14,"brick_affinity",20,"clearance",13,"love_brand",11,"hottest_x",
         12, "geo_style", 23, "geo_brand", 15, "love_color");

my @feeds = ( "53699_33838_date_LIVE_CAMPAIGN.csv",
	      "53699_33859_date_DCF_CAMPAIGN.csv",
	      "53699_54415_date_replenishment.csv",
	      "53699_78388_date_live_campaign_followup.csv");

foreach my $feed (@feeds) {
    my $feed_file = $feed;
    $feed_file =~ s/date/$date_with_zero/g;
    print "feed file is $feed_file\n";
    down_from_responsys($feed_file);
    if ($feed eq "53699_33838_date_LIVE_CAMPAIGN.csv") {
	count_campaigns($feed_file,3);
    } elsif ($feed eq "53699_33859_date_DCF_CAMPAIGN.csv") {
	count_campaigns($feed_file,9);
    } else {
	my $lc  = `wc -l $feed_file | cut -d ' ' -f 1`;
	print("Count ". $lc);
    }
    

}


# create a map of campaign number and names

sub count_campaigns {
        my ($feed_file,$pos) = @_;
	my $counts = `cat $feed_file | cut -d ';' -f $pos | sort | uniq -c`;
	my @counta = split("\n", $counts);

	foreach $count (@counta) {
	    chomp($count);
	    my @s = split(" ", $count);
	    if ( exists($data{$s[1]}) ) {
		print $data{$s[1]} . " " . $s[0] ." \n";
	    } else {
		print $s[1] . " " . $s[0] ." \n";
	    }
	}
}
