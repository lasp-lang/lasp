Lasp: A Language for Distributed, Eventually Consistent Computations
=======================================================

[![Build Status](https://travis-ci.org/lasp-lang/lasp.svg?branch=master)](https://travis-ci.org/lasp-lang/lasp)

To build:

* `make devrel`: Build six development releases.
* `make stagedevrel`: Build six development releases, symlinked to the same source.

To configure for `riak_test`:

* Build development releases: `make stagedevrel`
* Run the setup utility, once: `riak_test/bin/lasp-setup.sh`
* As you modify your code, run `make && riak_test/bin/lasp-current.sh`; this will rebuild your local source and then configure `riak_test` to use the latest version.

To run the tests:

* `make riak-test` to run all integration and program tests.
* `./rebar skip_deps=true eqc` to run the Erlang QuickCheck tests.
