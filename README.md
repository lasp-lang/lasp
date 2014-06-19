derflow: Distributed deterministic dataflow programming
=======================================================

To build:

* `make devrel`: Build six development releases.
* `make stagedevrel`: Build six development releases, symlinked to the
  same source.

To configure for `riak_test`:

* Build development releases: `make stagedevrel`
* Run the setup utility, once: `riak_test/bin/derflow-setup.sh`
* As you modify your code, run `make &&
  riak_test/bin/derflow-current.sh`; this will rebuild your local source
  and then configure `riak_test` to use the latest version.

TODO:

* Assert FSMs throw exception if they don't receive all of the same
  value.
* Fix handoff.
