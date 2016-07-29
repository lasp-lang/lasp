PACKAGE         ?= lasp
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
MAKE						 = make

.PHONY: rel deps test plots dcos

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

##
## Test targets
##

check: test xref dialyzer lint

test: ct eunit simulations

lint:
	${REBAR} as lint lint

eunit:
	${REBAR} as test eunit

ct:
	${REBAR} as test ct --suite=lasp_SUITE

simulations: client-server-ad-counter-simulation peer-to-peer-ad-counter-simulation ad-counter-divergence

peer-to-peer-ad-counter-simulation:
	${REBAR} as test ct --suite=lasp_peer_to_peer_advertisement_counter_SUITE

client-server-ad-counter-simulation:
	${REBAR} as test ct --suite=lasp_client_server_advertisement_counter_SUITE

ad-counter-divergence:
	${REBAR} as test ct --suite=lasp_advertisement_counter_divergence_SUITE

##
## Release targets
##

rel:
	${REBAR} release

stage:
	${REBAR} release -d

##
## Packaging targets
##

package: rel
	fpm -s dir -t deb -n $(PACKAGE) -v $(VERSION) \
	    --deb-user $(PACKAGE) \
	    --deb-group $(PACKAGE) \
	    --before-install=rel/before-install \
	    _build/default/rel/$(PACKAGE)=/opt/ \
	    rel/init=/etc/init.d/$(PACKAGE) \
	    rel/var/lib/$(PACKAGE)/=/var/lib/$(PACKAGE)/ \
	    rel/var/log/$(PACKAGE)/=/var/log/$(PACKAGE)/ \
	    rel/etc/$(PACKAGE)/$(PACKAGE).config=/etc/$(PACKAGE)/$(PACKAGE).config \
	    rel/etc/default/$(PACKAGE)=/etc/default/$(PACKAGE)

package_cloud:
	docker build -f Dockerfiles/packager -t cmeiklejohn/packager .
	docker run -i -t -v ~/.packagecloud:/root/.packagecloud cmeiklejohn/packager

cut:
	${REBAR} as package hex cut

publish:
	${REBAR} as package hex publish

shell:
	${REBAR} shell --apps lasp

dcos:
	bin/dcos-deploy.sh

##
## Evaluation related targets
##
plots:
	pkill -9 beam.smp; \
	  clear; \
		rm -rf priv/lager/ priv/evaluation; \
		(cd priv/ && git clone https://github.com/lasp-lang/evaluation); \
		./rebar3 ct --readable=false --suite=test/lasp_peer_to_peer_advertisement_counter_SUITE; \
		./rebar3 ct --readable=false --suite=test/lasp_client_server_advertisement_counter_SUITE; \
		cd priv/evaluation && make plots

div:
	pkill -9 beam.smp; \
		clear; \
		./rebar3 ct --readable=false --suite=test/lasp_advertisement_counter_divergence_SUITE

evaluate-local: SHELL:=/bin/bash
evaluate-local:
	priv/evaluate-local.sh

evaluate-docker: SHELL:=/bin/bash
evaluate-docker:
	priv/evaluate-docker.sh

logs:
	tail -F priv/lager/*/log/*.log

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
