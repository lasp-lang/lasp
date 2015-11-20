REPO            ?= lasp
PKG_REVISION    ?= $(shell git describe --tags)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
OVERLAY_VARS    ?=
REBAR            = $(shell pwd)/rebar3

.PHONY: rel deps test

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

##
## Test targets
##

test: ct eunit eqc

lint:
	./rebar3 as lint lint

eqc:
	./rebar3 as test eqc

eunit:
	./rebar3 eunit

ct:
	./rebar3 ct

##
## Release targets
##

rel:
		./rebar3 release

stage:
		./rebar3 release -d

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
