
VERSION = $(shell grep version build.sbt | cut -d '=' -f 2 | tr -d ' '| sed 's/"//g')
BASE = alchemy-core-$(VERSION)
PKGNAME = $(BASE).tar.gz

package:
	rm -rf $(BASE)
	rm -f $(PKGNAME)
	sbt assembly
	mkdir -p $(BASE)/bin
	mkdir -p $(BASE)/jar
	mkdir -p $(BASE)/conf
	cp target/scala-2.10/Alchemy-assembly-$(VERSION).jar $(BASE)/jar/
	cp src/main/resources/bobAcquisitionTables.json $(BASE)/conf/
	cp scripts/* $(BASE)/bin/
	tar -zcvf $(PKGNAME) $(BASE)
