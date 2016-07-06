PACKAGE         ?= lasp
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
MAKE						 = make

.PHONY: rel deps test eqc plots dcos

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

test: ct eunit

lint:
	${REBAR} as lint lint

eqc:
	${REBAR} as test eqc

eunit:
	${REBAR} as test eunit

ct:
	${REBAR} as test ct

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
ads: SHELL:=/bin/bash
ads:
		priv/evaluate.sh

logs:
	tail -F priv/lager/*/log/*.log

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
