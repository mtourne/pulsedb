VERSION := $(shell ./deploy/version.erl)

DEBIAN =  -s dir --url http://flussonic.com/ --description "Pulse collecting server" \
-m "Max Lapshin <max@flussonic.com>" --vendor "Flussonic, LLC" --license MIT \
--post-install ../deploy/postinst --pre-uninstall ../deploy/prerm --post-uninstall ../deploy/postrm \
--config-files /etc/pulsedb/pulsedb.config

FPM = ../deploy/epm.erl



all:
	./rebar get-deps compile

app:
	@./rebar compile

clean:
	@./rebar clean
	@rm -f erl_crash.dump

run:
	ERL_LIBS=..:deps erl -name pulsedb@127.0.0.1 -pa ebin -s pulsedb_launcher

ct: test

tmproot:
	rm -rf tmproot
	mkdir -p tmproot/opt/pulsedb
	git archive $(BRANCH) | (cd tmproot/opt/pulsedb; tar x)
	mkdir -p tmproot/opt/pulsedb/deps
	@# Dont copy from github, just make local clone. Later .git will be deleted
	[ -d deps ] && for d in deps/* ; do git clone $$d tmproot/opt/pulsedb/deps/`basename $$d`; done || true

	@# Now compile everything with regular build
	(cd tmproot/opt/pulsedb/ && ./rebar update-deps get-deps && ./rebar -j 8 compile)

	mkdir -p tmproot/etc/init.d/
	cp -f priv/pulsedb tmproot/etc/init.d/pulsedb
	mkdir -p tmproot/etc/pulsedb
	cp priv/pulsedb.config.sample tmproot/etc/pulsedb/pulsedb.config


package: tmproot
	cd tmproot && $(FPM) -f -t deb -n pulsedb -v $(VERSION) $(DEBIAN) -a amd64 --category net etc/init.d etc/pulsedb opt && cd ..
	cd tmproot && $(FPM) -f -t rpm -n pulsedb -v $(VERSION) $(DEBIAN) -a amd64 --gpg max@flussonic.com --category Server/Video etc/init.d etc/pulsedb opt && cd ..
	mv tmproot/*.deb tmproot/*.rpm .



test:
	mkdir -p logs
	ct_run -pa ebin -pa deps/*/ebin -logdir logs/ -dir test/

.PHONY: test

