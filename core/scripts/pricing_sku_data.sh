#!/usr/bin/env bash
perl /opt/alchemy-core/current/bin/run.pl -t prod -c pricingSKUData
perl /opt/alchemy-core/current/bin/ftp_upload.pl -c pricing_sku_data