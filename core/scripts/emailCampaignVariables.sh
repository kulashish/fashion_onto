#!/usr/bin/env bash

# call custWelcomeVoucher
perl /opt/alchemy-core/current/bin/run.pl -t TEST-PROD -c custWelcomeVoucher

# ftp upload
perl /opt/alchemy-core/current/bin/ftp_upload.pl -c custWelcomeVoucher

# call custPreference
perl /opt/alchemy-core/current/bin/run.pl -t TEST-PROD -c custPreference

# ftp upload
perl /opt/alchemy-core/current/bin/ftp_upload.pl -c custPreference

# call contactListMobile
perl /opt/alchemy-core/current/bin/run.pl -t TEST-PROD -c contactListMobile

# ftp upload
perl /opt/alchemy-core/current/bin/ftp_upload.pl -c contactListMobile

