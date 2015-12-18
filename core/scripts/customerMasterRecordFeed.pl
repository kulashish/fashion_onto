#!/usr/bin/env perl

# call customer master record feed
system("perl /opt/alchemy-core/current/bin/run.pl -t PROD -c customerMasterRecordFeed");

# copy processed data to ftp location
system("perl /opt/alchemy-core/current/bin/ftp_upload.pl -t PROD -c ad4push_customer_response");
