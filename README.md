Distributed deterministic dataflow programming
=======================================================

To build:

* `make devrel`: Build six development releases.
* `make stagedevrel`: Build six development releases, symlinked to the same source.

To configure for `riak_test`:

* Build development releases: `make stagedevrel`
* Run the setup utility, once: `riak_test/bin/derflow-setup.sh`
* As you modify your code, run `make && riak_test/bin/derflow-current.sh`; this will rebuild your local source and then configure `riak_test` to use the latest version.

To run the tests:

* `make riak-test` to run all integration and program tests.
* `./rebar skip_deps=true eqc` to run the Erlang QuickCheck tests.
