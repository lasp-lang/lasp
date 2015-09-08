Lasp
=======================================================

[![Build Status](https://travis-ci.org/lasp-lang/lasp.svg?branch=master)](https://travis-ci.org/lasp-lang/lasp)

## Overview

Lasp is a Language for Distributed, Eventually Consistent Computations.

## Building

To build:

* `make devrel`: Build six development releases.
* `make stagedevrel`: Build six development releases, symlinked to the same source.

## Testing

* `make test` will run the unit and QuickCheck tests.
* `make riak-test` will run the integration tests.

## Riak Core Backend

The Riak Core backend for Lasp is **deprecated**.  If you need it for
it, the last supported release is tagged `riak-core-distribution`.
