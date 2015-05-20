Lasp: A Language for Distributed, Eventually Consistent Computations with CRDTs
=======================================================

Curious about what we're building?  Try out these posts:

* [Programming Models, Part 1: Try Derflow!](http://christophermeiklejohn.com/derflow/erlang/2014/09/28/try-derflow.html)
* [Programming Models, Part 2: QuickChecking Derflow](http://christophermeiklejohn.com/derflow/erlang/2014/10/01/quickchecking-derflow.html)
* [Programming Models, Part 3: Ad Counter, Part 1](http://christophermeiklejohn.com/derflow/erlang/2014/11/16/ad-counter-derflow.html)
* [Programming Models, Part 4: One Week in Louvain-la-Neuve](http://christophermeiklejohn.com/erlang/lasp/2014/12/21/lasp.html)
* [Programming Models, Part 5: Ad Counter, Part 2](http://christophermeiklejohn.com/lasp/erlang/2015/01/10/ad-counter-orset.html)
* [Highly Distributed Computations Without Synchronization](http://christophermeiklejohn.com/lasp/erlang/2015/02/18/infoq.html)
* [Seminars on Lasp in March, 2015](http://christophermeiklejohn.com/lasp/erlang/2015/02/27/seminars.html)

We also have a workshop paper:

* [Lasp: a language for distributed, eventually consistent computations with CRDTs](http://dl.acm.org/citation.cfm?id=2745954)

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
