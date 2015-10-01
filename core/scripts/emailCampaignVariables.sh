#!/usr/bin/env bash

# call custWelcomeVoucher
perl /opttest/alchemy-core/current/bin/run.pl -t TEST-PROD -c custWelcomeVoucher

# ftp upload
perl /opttest/alchemy-core/current/bin/ftp_upload.pl -c custWelcomeVoucher

# call custPreference
perl /opttest/alchemy-core/current/bin/run.pl -t TEST-PROD -c custPreference

# ftp upload
perl /opttest/alchemy-core/current/bin/ftp_upload.pl -c custPreference

# call contactListMobile
perl /opttest/alchemy-core/current/bin/run.pl -t TEST-PROD -c contactListMobile

# ftp upload
perl /opttest/alchemy-core/current/bin/ftp_upload.pl -c contactListMobile

