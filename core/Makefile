
VERSION = $(shell grep version build.sbt | cut -d '=' -f 2 | tr -d ' '| sed 's/"//g')
BASE = alchemy-core-$(VERSION)
PKGNAME = $(BASE).tar.gz

package:
	rm -rf $(BASE)
	rm -f $(PKGNAME)
	sbt clean assembly
	mkdir -p $(BASE)/bin/clickstream
	mkdir -p $(BASE)/jar
	mkdir -p $(BASE)/conf
	mkdir -p $(BASE)/workflows
	cp target/scala-2.10/Alchemy-assembly.jar $(BASE)/jar/
	cp src/main/resources/*.json $(BASE)/conf/
	cp scripts/confInit.sh $(BASE)/bin/
	cp scripts/confInit_jabongtest.sh $(BASE)/bin/
	cp scripts/run.pl $(BASE)/bin/
	cp scripts/ftp_upload.pl $(BASE)/bin/
	cp scripts/ad4push_reaction.pl $(BASE)/bin/
	cp scripts/ad4push_device_merger.pl $(BASE)/bin/
	cp scripts/responsys.pl $(BASE)/bin/
	cp -r oozie/* $(BASE)/workflows/
	cp scripts/clickstream/* $(BASE)/bin/clickstream/
	cp scripts/ftp_decrypt.pl $(BASE)/bin/
	tar -zcvf $(PKGNAME) $(BASE)
