%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(lasp).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("lasp.hrl").

-export([bind/4,
         declare/1,
         declare_dynamic/1]).

-export([query/1,
         declare/2,
         declare_dynamic/2,
         update/3,
         bind/2,
         bind_to/2,
         read/2,
         read_any/1,
         filter/3,
         map/3,
         product/3,
         union/3,
         intersection/3,
         fold/3,
         wait_needed/2,
         thread/3]).

%% Public Helpers

%% @doc Return the current value of a CRDT.
%%
%%      For a given `Id', compute the current value of a CRDT and return
%%      it.
%%
-spec query(id()) -> {ok, term()} | error().
query(Id) ->
    do(query, [Id]).

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
-spec declare(type()) -> {ok, id()} | {error, timeout}.
declare(Type) ->
    declare(druuid:v4(), Type).

%% @doc Declare a new dynamic variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
-spec declare_dynamic(type()) -> {ok, id()} | {error, timeout}.
declare_dynamic(Type) ->
    declare_dynamic(druuid:v4(), Type).

%% @doc Declare a new dynamic variable of a given type and identifier.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
-spec declare_dynamic(id(), type()) -> {ok, id()} | {error, timeout}.
declare_dynamic(Id, Type) ->
    do(declare_dynamic, [Id, Type]).

%% @doc Bind a dataflow variable to the result of a function call.
%%
%%      Execute `Module:Function(Args)' and bind the result using {@link
%%      bind/2}.
%%
-spec bind(id(), module(), func(), args()) -> {ok, id()} | {error, timeout}.
bind(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

%% Public API

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
%%      Type is declared with the provided `Id'.
%%
-spec declare(id(), type()) -> {ok, id()} | {error, timeout}.
declare(Id, Type) ->
    do(declare, [Id, Type]).

%% @doc Update a dataflow variable.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor()) -> {ok, {value(), id()}} | {error, timeout}.
update(Id, Operation, Actor) ->
    do(update, [Id, Operation, Actor]).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, id()} | {error, timeout}.
bind(Id, Value) ->
    do(bind, [Id, Value]).

%% @doc Bind a dataflow variable to another dataflow variable.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind_to(id(), id()) -> {ok, id()} | {error, timeout}.
bind_to(Id, TheirId) ->
    do(bind_to, [Id, TheirId]).

%% @doc Blocking monotonic read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound, and
%%      is monotonically greater (as defined by the lattice) then the
%%      provided `Threshold' value.
%%
-spec read(id(), threshold()) -> {ok, var()} | {error, timeout}.
read(Id, Threshold) ->
    do(read, [Id, Threshold]).

%% @doc Blocking monotonic read operation for a list of given dataflow
%%      variables.
%%
-spec read_any([{id(), threshold()}]) -> {ok, var()} | {error, timeout}.
read_any(Reads) ->
    do(read_any, [Reads]).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id()) -> ok | {error, timeout}.
product(Left, Right, Product) ->
    do(product, [Left, Right, Product]).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id()) -> ok | {error, timeout}.
union(Left, Right, Union) ->
    do(union, [Left, Right, Union]).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id()) -> ok | {error, timeout}.
intersection(Left, Right, Intersection) ->
    do(intersection, [Left, Right, Intersection]).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id()) -> ok | {error, timeout}.
map(Id, Function, AccId) ->
    do(map, [Id, Function, AccId]).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id()) -> ok | {error, timeout}.
fold(Id, Function, AccId) ->
    do(fold, [Id, Function, AccId]).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id()) -> ok | {error, timeout}.
filter(Id, Function, AccId) ->
    do(filter, [Id, Function, AccId]).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args()) -> ok | {error, timeout}.
thread(Module, Function, Args) ->
    do(thread, [Module, Function, Args]).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), threshold()) -> ok | {error, timeout}.
wait_needed(Id, Threshold) ->
    do(wait_needed, [Id, Threshold]).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = distribution_backend(),
    erlang:apply(Backend, Function, Args).

distribution_backend() ->
    application:get_env(?APP,
                        distribution_backend,
                        lasp_plumtree_broadcast_distribution_backend).
