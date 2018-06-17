PACKAGE         ?= lasp
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
VERSION 	     = "0.8.2" 
MAKE						 = make

.PHONY: rel deps test plots dcos logs fpm

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

kill:
	pkill -9 beam.smp; \
	pkill -9 epmd; \
	exit 0

check: kill test xref dialyzer

test: ct eunit ad-counter-simulations game-tournament-simulations

lint:
	${REBAR} as lint lint

eunit:
	${REBAR} eunit

ct:
	${REBAR} ct --suite=lasp_SUITE

ad-counter-simulations: client-server-ad-counter-simulation peer-to-peer-ad-counter-simulation ad-counter-overcounting ad-counter-partition-overcounting
game-tournament-simulations: client-server-game-tournament-simulation peer-to-peer-game-tournament-simulation

peer-to-peer-ad-counter-simulation:
	${REBAR} ct --suite=lasp_peer_to_peer_advertisement_counter_SUITE

client-server-ad-counter-simulation:
	${REBAR} ct --suite=lasp_client_server_advertisement_counter_SUITE

ad-counter-overcounting:
	#${REBAR} ct --suite=lasp_advertisement_counter_overcounting_SUITE

ad-counter-partition-overcounting:
	${REBAR} ct --suite=lasp_advertisement_counter_partition_overcounting_SUITE

peer-to-peer-game-tournament-simulation:
	${REBAR} ct --suite=lasp_peer_to_peer_game_tournament_SUITE

client-server-game-tournament-simulation:
	${REBAR} ct --suite=lasp_client_server_game_tournament_SUITE

peer-to-peer-throughput-simulation:
	${REBAR} ct --suite=lasp_peer_to_peer_throughput_SUITE

client-server-throughput-simulation:
	${REBAR} ct --suite=lasp_client_server_throughput_SUITE

client-server-divergence-simulation:
	${REBAR} as test ct --suite=lasp_client_server_divergence_SUITE

##
## Release targets
##

rel:
	${REBAR} release

stage:
	${REBAR} release -d

exp-rel:
	${REBAR} as exp release

exp-stage:
	${REBAR} as exp release -d

##
## Packaging targets
##

fpm:
	gem install --no-ri --no-rdoc fpm

package: rel fpm
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

package_cloud: package
	docker build -f Dockerfiles/packager -t cmeiklejohn/packager .
	docker run -i -t -v ~/.packagecloud:/root/.packagecloud cmeiklejohn/packager

cut:
	${REBAR} as package hex cut

publish:
	${REBAR} as package hex publish

shell:
	${REBAR} shell --apps lasp --name lasp@localhost

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
		./rebar3 ct --readable=false --suite=test/lasp_advertisement_counter_overcounting_SUITE

part-div:
	pkill -9 beam.smp; \
		clear; \
		./rebar3 ct --readable=false --suite=test/lasp_advertisement_counter_partition_overcounting_SUITE

tail-logs:
	tail -F priv/lager/*/*/log/*.log

logs:
	cat priv/lager/*/*/log/*.log

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

crdt-tutorial: stage
	tmux -f tmux.conf kill-server; tmux -f crdt-tmux.conf start-server; tmux -f crdt-tmux.conf attach -d -t lasp

partisan-tutorial: stage
	tmux -f tmux.conf kill-server; tmux -f partisan-tmux.conf start-server; tmux -f partisan-tmux.conf attach -d -t lasp

include tools.mk
