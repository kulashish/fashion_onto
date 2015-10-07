
VERSION = $(shell grep version build.sbt | cut -d '=' -f 2 | tr -d ' '| sed 's/"//g')
BASE = alchemy-core-$(VERSION)
PKGNAME = $(BASE).tar.gz

package:
	rm -rf $(BASE)
	rm -f $(PKGNAME)
	sbt clean assembly
	mkdir -p $(BASE)/bin
	mkdir -p $(BASE)/jar
	cp target/scala-2.10/Alchemy-adjust.jar $(BASE)/jar/
	cp scripts/adjust.pl $(BASE)/bin/
	tar -zcvf $(PKGNAME) $(BASE)
