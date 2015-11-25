PACKAGE         ?= lasp
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3

.PHONY: rel deps test eqc

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
	./rebar3 as lint lint

eqc:
	./rebar3 as test eqc

eunit:
	./rebar3 as test eunit

ct:
	./rebar3 as test ct

##
## Release targets
##

rel:
		./rebar3 release

stage:
		./rebar3 release -d

##
## Packaging targets
##

package: rel
	fpm -s dir -t deb -n $(PACKAGE) -v $(VERSION) \
	    --before-install=rel/before-install \
	    _build/default/rel/$(PACKAGE)=/opt/ \
	    rel/init=/etc/init.d/$(PACKAGE) \
	    rel/var/lib/$(PACKAGE)/=/var/lib/$(PACKAGE)/ \
	    rel/etc/$(PACKAGE)/$(PACKAGE).config=/etc/$(PACKAGE)/$(PACKAGE).config \
	    rel/etc/default/$(PACKAGE)=/etc/default/$(PACKAGE)

cut:
	./rebar3 as package hex cut

publish:
	./rebar3 as package hex publish

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
