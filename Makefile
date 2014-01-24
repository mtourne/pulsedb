VERSION := $(shell ./deploy/version.erl)

DEBIAN =  -s dir --url http://flussonic.com/ --description "Pulse collecting server" \
-m "Max Lapshin <max@flussonic.com>" --vendor "Flussonic, LLC" --license MIT \
--post-install ../deploy/postinst --pre-uninstall ../deploy/prerm --post-uninstall ../deploy/postrm \
--config-files /etc/pulsedb/pulsedb.config

FPM = ../deps/epm/epm.erl



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
	git archive master | (cd tmproot/opt/pulsedb; tar x)
	mkdir -p tmproot/opt/pulsedb/deps
	@# Dont copy from github, just make local clone. Later .git will be deleted
	[ -d deps ] && for d in deps/* ; do git clone -q $$d tmproot/opt/pulsedb/deps/`basename $$d` 2>/dev/null; done || true

	@# Now compile everything with regular build
	(cd tmproot/opt/pulsedb/ && ./rebar update-deps get-deps && ./rebar -j 8 compile)

	@rm -rf tmproot/opt/pulsedb/deps/*/.git
	@rm -rf tmproot/opt/pulsedb/deps/*/test
	@rm -rf tmproot/opt/pulsedb/deps/*/examples
	@rm -rf tmproot/opt/pulsedb/deps/meck
	@rm -rf tmproot/opt/pulsedb/deps/neotoma
	@rm -rf tmproot/opt/pulsedb/deps/epm
	@rm -rf tmproot/opt/pulsedb/test
	@rm -rf tmproot/opt/pulsedb/priv
	@rm -rf tmproot/opt/pulsedb/deploy
	@rm -rf tmproot/opt/pulsedb/Vagrantfile
	@rm -rf tmproot/opt/pulsedb/Makefile
	@rm -rf tmproot/opt/pulsedb/Emakefile

	mkdir -p tmproot/etc/init.d/
	cp -f priv/pulsedb tmproot/etc/init.d/pulsedb
	mkdir -p tmproot/etc/pulsedb
	cp priv/pulsedb.config.sample tmproot/etc/pulsedb/pulsedb.config


package: tmproot
	cd tmproot && $(FPM) -f -t deb -n pulsedb -v $(VERSION) $(DEBIAN) -a amd64 --category net etc/init.d etc/pulsedb opt && cd ..
	mv tmproot/*.deb .
	@#cd tmproot && $(FPM) -f -t rpm -n pulsedb -v $(VERSION) $(DEBIAN) -a amd64 --gpg max@flussonic.com --category Server/Video etc/init.d etc/pulsedb opt && cd ..
	@#mv tmproot/*.rpm .
	rm -rf tmproot


test:
	mkdir -p logs
	ct_run -pa ebin -pa deps/*/ebin -logdir logs/ -dir test/

.PHONY: test tmproot

