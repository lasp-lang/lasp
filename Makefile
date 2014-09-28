REBAR = $(shell pwd)/rebar
.PHONY: rel deps test

all: deps compile compile-riak-test

compile: deps
	$(REBAR) compile

compile-riak-test: compile
	$(REBAR) skip_deps=true riak_test_compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean
	rm -rf riak_test/ebin

distclean: clean devclean relclean
	$(REBAR) delete-deps

rel: all
	$(REBAR) generate

relclean:
	rm -rf rel/derflow

stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/derflow/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/derflow/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/derflow/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/derflow/lib;)

currentdevrel: stagedevrel
	riak_test/bin/derflow-current.sh

riak-test:
	$(foreach dep,$(wildcard riak_test/*_test.erl), ../riak_test/riak_test -v -c derflow -t $(dep);)

riak-test-lattice: currentdevrel
	../riak_test/riak_test -v -c derflow -t derflow_lattice_test

riak-test-threshold: currentdevrel
	../riak_test/riak_test -v -c derflow -t derflow_threshold_read_test

riak-test-programs: compile compile-riak-test
	../riak_test/riak_test -v -c derflow -t derflow_programs_test

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
DEVNODES ?= 6

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)
	  $(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)

devclean: clean
	rm -rf dev

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
