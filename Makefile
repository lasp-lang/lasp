REPO            ?= lasp
PKG_REVISION    ?= $(shell git describe --tags)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
OVERLAY_VARS    ?=
REBAR            = $(shell pwd)/rebar3
REBAR2           = $(shell pwd)/rebar

.PHONY: rel deps test

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean testclean devclean relclean packageclean

generate:
	$(REBAR) generate $(OVERLAY_VARS)

##
## Release targets
##

rel:
		./rebar3 release

stage:
		./rebar3 release -d

##
## Test targets
##

check: xref dialyzer test riak-test

testdeps: deps
	$(REBAR) as test compile

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto riak_dt

include tools.mk

include package.mk

include riak_test.mk
