use Net::FTP;
#use File::Map qw(map_file); # http://cpansearch.perl.org/src/LEONT/File-Map-0.42/INSTALL
use POSIX qw(strftime);

$ENV{'YARN_CONF_DIR'} = '/etc/hadoop/conf';
print $ENV{'YARN_CONF_DIR'};
my $user = "jabong";

my @events = ('add_to_cart',  'add_to_wishlist', 'click', 'customer', 'fb_connect', 'install', 'launch', 'login', 'logout',
 'product_rate', 'reattribution', 'remove_from_cart', 'remove_from_wishlist', 'sale', 'session', 'signup', 'social_share');

my $cleaning = 1; # 1/0
my @headers = ("os", "environment", "user_id", "campaign", "adid", "google_play_advertising_id", "store", "reftag",  "adgroup", "creative",
    "transaction_id", "payment_option_selected", "revenue", "price", "sku", "skus",
    "ip", "country", "shop_country", "currency_code", "os_version", "network", "network_class",
    "device", "DeviceID", "device_id", "wp_device_id", "device_model", "device_name", "device_manufacturer", "display_size",
    "bundle", "app_version", "sdk_version", "user_agent", "session", "language", "tracking_enabled",
    "time", "install_time", "Date", "time_spent", "timezone", "click_time", "duration", "last_time_spent",
    "fb_campaign_name", "fb_campaign_id", "fb_adset_name", "fb_adset_id", "fb_ad_name", "fb_ad_id"
 );

my @apps_list = ("android-android", "ios-ios", "wp8-windows_phone");
my %criteria_list = (1 => "event_wise", 2 => "app_wise", 3 => "specific_app");
my $criteria = "event_wise";
my $specific_app = "";

for (my $day = 1; $day < 2; $day++) {
    my $last_n_days = $day;
    my $date_for_raw_files = strftime "%Y%m%d", localtime(time() - 60*60*24*$last_n_days);
    my $date_for_dirs = strftime "%Y/%m/%d", localtime(time() - 60*60*24*$last_n_days);

    for (my $i = 0; $i < scalar(@apps_list); $i++) {
    my $app = "";
    my $date_for_dirs = strftime "%Y/%m/%d", localtime(time() - 60*60*24*$last_n_days);
    if ($criteria eq $criteria_list{1}) {
        $app = "*";
        $date_for_dirs = "$date_for_dirs/event";
        if ($i > 0) { last; }
    } elsif ($criteria eq $criteria_list{2}) {
        $app = $apps_list[$i];
        $date_for_dirs = "$date_for_dirs/$app";
    } elsif ($criteria eq $criteria_list{3}) {
        if (length $specific_app) {
            $app = $specific_app;
            if ($i > 0) { last; }
        } else {
            die "$! \nSpecify the app name: \n";
        }
        $date_for_dirs = "$date_for_dirs/$app";
    }

    my %config = (
     's3_bucket' => "jabong-events-input",
     's3_file_format' => "$date_for_raw_files*com.jabong.$app.log.gz",
     's3_profile' => "adjust",
     'raw_files_download_path' => "/data/adjust/rawData/$date_for_dirs/",
     'hdfs_files_input_path' => "/data/input/adjust/$date_for_dirs/",
     'hdfs_files_output_path' => "/data/output/adjust/$date_for_dirs/",
     'hdfs_job_jar_file' => "/opt/alchemy-adjust/current/jar/Alchemy-adjust.jar",
     'ftp_files_input_path' => "/data/adjust/csv/$date_for_dirs/",
     'ftp_files_output_path' => "/adjust/dap/csv/$date_for_dirs/",
     'ftp_location' =>   "61.16.155.203",
     'ftp_user' =>   "FT0072",
     'ftp_passwd' =>  "J\@de\$h\@re"
    );

    print "\nDate: ".localtime(time())."\nApp: $app\n";
    print "!!! Starting Adjust ETL job for $date_for_dirs !!!\n\n";

    # Delete all directories/files resursively, if already exists
    print "Cleaning environment...\nRemove directories/files if already exists.\n";
    system("rm -rf $config{'raw_files_download_path'}");
    system("hdfs dfs -rm -r $config{'hdfs_files_input_path'}");
    system("hdfs dfs -rm -r $config{'hdfs_files_output_path'}");
    system("rm -rf $config{'ftp_files_input_path'}");
    print "\n" . localtime(time()) . "\n";

    print "\n################### Start: Data Extraction ##################\n";

    # Check event logs in S3
    print "\nChecking any event files available for processing for $date_for_raw_files in bucket: s3://$config{'s3_bucket'} ... \n";
    my $raw_files_count = `aws s3 ls --profile "$config{'s3_profile'}" s3://"$config{'s3_bucket'}" | grep '$date_for_raw_files' | tail`;
    #my $raw_files_count = system("aws s3 ls --profile $config{'s3_profile'} s3://$config{'s3_bucket'} | tail");
    print "\n" . localtime(time()) . "\n";
    unless($raw_files_count) {
     print "No event files available for date: $date_for_raw_files\n";
     exit 0;
    }
    print "Listing tail of event files:\n$raw_files_count\n";

    print "Creating directory $config{'raw_files_download_path'} to download event logs\n";
    if ( !-d "$config{'raw_files_download_path'}" ) {
     system(`mkdir -p "$config{'raw_files_download_path'}"`);
      if (!-d "$config{'raw_files_download_path'}") {
       die "$! \nError: Failed to create path $config{'raw_files_download_path'} to store event files.";
      }
    }

    # Download event logs
    print "Wait while files are being downloaded....\n";
    system(`aws s3 cp --profile "$config{'s3_profile'}" s3://"$config{'s3_bucket'}/"  "$config{'raw_files_download_path'}" --recursive --exclude "*" --include "$config{'s3_file_format'}"`);
    print "\n" . localtime(time()) . "\n";
    my $count_event_files = `find $config{'raw_files_download_path'} -type f -name '*.log.gz' | wc -l`;
    if ($count_event_files eq 0) {
     print "Error: Unable to fetch event files\n";
     exit 0;
    }
    print "Event log files fetched successfully.\n";

    # Move event log files to hdfs
    print "Copy event log files to hdfs.\n";
    system("hdfs dfs -mkdir -p $config{'hdfs_files_input_path'}");
    system("hdfs dfs -put $config{'raw_files_download_path'}*  $config{'hdfs_files_input_path'}/ ");
    print "\n" . localtime(time()) . "\n";
    print "\n################### End: Data Extraction ##################\n";


    print "\n################### Start: Data Transformation ##################\n";
    `/ext/spark/bin/spark-submit --class "com.jabong.dap.adjust.CsvCreator"  --master yarn-cluster   --num-executors 10 --executor-memory 4G --conf spark.akka.frameSize=100 --verbose --jars /ext/spark/lib/datanucleus-api-jdo-3.2.6.jar,/ext/spark/lib/datanucleus-core-3.2.10.jar,/ext/spark/lib/datanucleus-rdbms-3.2.9.jar --files /ext/spark/conf/hive-site.xml  "$config{'hdfs_job_jar_file'}" "$config{'hdfs_files_input_path'}"  "$config{'hdfs_files_output_path'}"`;
    print "\n" . localtime(time()) . "\n";

    system("mkdir -p $config{'ftp_files_input_path'}");
    print "Copying all processed event files from hdfs to local filesystem in csv format without headers.\n";
    print "Location: $config{'ftp_files_input_path'}\n";
    foreach $event (@events) {
     print "$event\_tmp.csv .... ";
     system("hdfs dfs -copyToLocal $config{'hdfs_files_output_path'}$event  $config{'ftp_files_input_path'}$event\_tmp.csv");
     print "Done\n";
     print localtime(time()) . "\n";
    }
    print "\n" . localtime(time()) . "\n";
    print "\n################### End: Data Transformation ##################\n";


    print "\n################### Start: Data Loading ##################\n";
    print "Copy all the csv files to remote location via FTP\n";
    $ftp = Net::FTP->new("$config{'ftp_location'}", Debug => 0)
     or die "Unable to connect to ftp location: $config{'ftp_location'}: $@";
    $ftp->login("$config{'ftp_user'}", "$config{'ftp_passwd'}")
     or die "Unable to login to ftp with user: $config{'ftp_user'}", $ftp->message;
    $ftp->mkdir("$config{'ftp_files_output_path'}", true)
     or die "Unable to create ftp directory: $config{'ftp_files_output_path'} ", $ftp->message;
    $ftp->cwd("$config{'ftp_files_output_path'}")
     or die "Unable to change to ftp working directory: $config{'ftp_files_output_path'}", $ftp->message;

    my $headers_string = join(',', @headers);
    foreach $event (@events) {
     print "$event\.csv .... ";

     #map_file my $map_csv_file, "$config{'ftp_files_input_path'}$event\_tmp.csv";
     `echo "$headers_string" > "$config{'ftp_files_input_path'}$event\.csv"`;
     `cat "$config{'ftp_files_input_path'}$event\_tmp.csv" >> "$config{'ftp_files_input_path'}$event\.csv"`;
     `rm "$config{'ftp_files_input_path'}$event\_tmp.csv"`;

     $ftp->put("$config{'ftp_files_input_path'}$event\.csv")
      or die "Failed ftp uploading file: $config{'ftp_files_input_path'}$event\.csv", $ftp->message;
     print "uploaded\n";
     print "\n" . localtime(time()) . "\n";
    }
    $ftp->quit;
    print "\n################### End: Data Loading ##################\n";

    # Remove all dirs/files not required any more
    if ($cleaning eq 1) {
      print "Cleaning process started....\n";
      print "Removing all directories/files which are not required any more\n\n";
      system("rm -rf $config{'raw_files_download_path'}");
      system("hdfs dfs -rm -r $config{'hdfs_files_input_path'}");
      system("hdfs dfs -rm -r $config{'hdfs_files_output_path'}");
      system("rm -rf $config{'ftp_files_input_path'}")
    }

}

    print "\n" . localtime(time()) . "\n";
    print "!!! Ending Adjust ETL job for $date_for_dirs !!!\n";
}
exit;
