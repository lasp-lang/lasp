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

compile-riak-test: compile testdeps escriptize
	$(REBAR2) riak_test_compile skip_deps=true

clean:
	$(REBAR) clean
	rm -rf riak_test/ebin/*.beam

distclean: clean devclean relclean packageclean

generate:
	$(REBAR) generate $(OVERLAY_VARS)

##
## Release targets
##
rel: compile generate

relclean:
	rm -rf rel/lasp

stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/lasp/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/lasp/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/lasp/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/lasp/lib;)

##
## Test targets
##

check: xref dialyzer test riak-test

testdeps: deps
	$(REBAR) as test compile

##
## Riak Test targets
##

TEST_PATH=_build/test/lib

escriptize:
	(cd $(TEST_PATH)/riak_test && $(REBAR2) get-deps compile && $(REBAR2) escriptize skip_deps=true)

setup: stagedevrel
	riak_test/bin/lasp-setup.sh

current: stagedevrel
	riak_test/bin/lasp-current.sh

riak-test: compile compile-riak-test setup current
	$(foreach dep,$(wildcard riak_test/*_test.erl), $(TEST_PATH)/riak_test/riak_test -v -c lasp -t $(dep);)

##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

.PHONY : stagedevrel devrel
DEVNODES ?= 3

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) dev/$^/lib;)
	  $(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) dev/$^/lib;)

devclean: clean
	rm -rf dev

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk

include package.mk
