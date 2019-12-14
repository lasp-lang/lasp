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

## Creating a small cluster
Clone Lasp:
```
$ git clone https://github.com/lasp-lang/lasp.git
```

Run two shells
```
$ rebar3 shell --name a@127.0.0.1
$ rebar3 shell --name b@127.0.0.1
```

Exceute to node a:
```1> lasp_peer_service:join('a@127.0.0.1').
ok
2> lasp_peer_service:members().
{ok,['a@127.0.0.1','b@127.0.0.1']}
```

Execute node b:
```
1> lasp_peer_service:members().
{ok,['a@127.0.0.1','b@127.0.0.1']}     
```

Go back to node a and run:
```
3> Content = #{what => i_am_an_awmap_value}.

% create a lasp CRDT
AwMapVarName = <<"awmap">>.
Key1 = <<"key1">>.
AwMapType = {state_awmap, [state_mvregister]}.
{ok, {AwMap, _, _, _}} = lasp:declare({AwMapVarName, AwMapType}, AwMapType).


% Update the CRDT with the content
{ok, _} = lasp:update(AwMap, {apply, Key1, {set, nil, Content}}, self()).
```

Go to node b and retrieve the content of the CRDT:
```
2> {ok,[{_, AwMapSet}]} = lasp:query({<<"awmap">>,{state_awmap,[state_mvregister]}}).

3> sets:to_list(AwMapSet).
% [#{what => i_am_an_awmap_value}]
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

## Code examples

[This blog post](http://marianoguerra.org/posts/playing-with-lasp-and-crdts.html) by [@marianoguerra](https://github.com/marianoguerra) contains concise sample code.
