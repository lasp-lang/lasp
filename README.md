Lasp
=======================================================

[![Build Status](https://travis-ci.org/lasp-lang/lasp.svg?branch=master)](https://travis-ci.org/lasp-lang/lasp)

## Overview

Lasp is a programming model for synchronization-free computations.

## Installing

Lasp requires Erlang 19 or greater.  Once you have Erlang installed, do
the following to install and build Lasp.

```
$ git clone git@github.com:lasp-lang/lasp.git
$ cd lasp
$ make
```

## Running a shell

You can run a Erlang shell where you can interact with a Lasp node by
doing the following:

```
$ make shell
```

## Running the test suite

To run the test suite, which will execute all of the Lasp scenarios, use
the following command.

```
$ make check
```
