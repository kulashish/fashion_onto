perl /opt/alchemy-core/current/bin/run.pl -t prod -c dcfFeedGenerate
perl /opt/alchemy-core/bin/ftp_upload.pl -c dcf_feed
