%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_riak_core_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(lasp_distribution_backend).

-include("lasp.hrl").

-export([declare/2,
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

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.  Type is declared with the provided `Id'.
%%
-spec declare(id(), type()) -> {ok, id()} | error().
declare(Id, Type) ->
    {ok, ReqId} = lasp_declare_fsm:declare(Id, Type),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Update a dataflow variable.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor()) -> {ok, {value(), id()}} | error().
update(Id, Operation, Actor) ->
    {ok, ReqId} = lasp_update_fsm:update(Id, Operation, Actor),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, var()} | error().
bind(Id, Value) ->
    {ok, ReqId} = lasp_bind_fsm:bind(Id, Value),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Bind a dataflow variable to another dataflow variable.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind_to(id(), id()) -> {ok, id()} | error().
bind_to(Id, TheirId) ->
    {ok, ReqId} = lasp_bind_to_fsm:bind_to(Id, TheirId),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Blocking monotonic read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound, and
%%      is monotonically greater (as defined by the lattice) then the
%%      provided `Threshold' value.
%%
-spec read(id(), threshold()) -> {ok, var()} | error().
read(Id, Threshold) ->
    {ok, ReqId} = lasp_read_fsm:read(Id, Threshold),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Blocking monotonic read operation for a list of given dataflow
%%      variables.
%%
-spec read_any([{id(), threshold()}]) -> {ok, var()} | error().
read_any(Reads) ->
    ReqId = ?REQID(),
    _ = [lasp_read_fsm:read(Id, Threshold, ReqId) || {Id, Threshold} <- Reads],
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id()) -> ok | error().
product(Left, Right, Product) ->
    {ok, ReqId} = lasp_product_fsm:product(Left, Right, Product),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id()) -> ok | error().
union(Left, Right, Union) ->
    {ok, ReqId} = lasp_union_fsm:union(Left, Right, Union),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id()) -> ok | error().
intersection(Left, Right, Intersection) ->
    {ok, ReqId} = lasp_intersection_fsm:intersection(Left, Right, Intersection),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id()) -> ok | error().
map(Id, Function, AccId) ->
    {ok, ReqId} = lasp_map_fsm:map(Id, Function, AccId),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id()) -> ok | error().
fold(Id, Function, AccId) ->
    {ok, ReqId} = lasp_fold_fsm:fold(Id, Function, AccId),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id()) -> ok | error().
filter(Id, Function, AccId) ->
    {ok, ReqId} = lasp_filter_fsm:filter(Id, Function, AccId),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args()) -> ok | error().
thread(Module, Function, Args) ->
    {ok, ReqId} = lasp_thread_fsm:thread(Module, Function, Args),
    ?WAIT(ReqId, ?TIMEOUT).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), threshold()) -> ok | error().
wait_needed(Id, Threshold) ->
    {ok, ReqId} = lasp_wait_needed_fsm:wait_needed(Id, Threshold),
    ?WAIT(ReqId, ?TIMEOUT).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
