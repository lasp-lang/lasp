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

-include("lasp.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([declare/1,
         declare/2,
         update/3,
         bind/2,
         bind/4,
         bind_to/2,
         read/1,
         read/2,
         read_any/1,
         filter/3,
         map/3,
         product/3,
         union/3,
         intersection/3,
         fold/3,
         produce/2,
         produce/4,
         consume/1,
         extend/1,
         wait_needed/1,
         wait_needed/2,
         thread/3,
         preflist/3,
         get_stream/1,
         register/3,
         execute/2,
         process/6,
         mk_reqid/0,
         wait_for_reqid/2]).

%% Public API

%% @doc Register an application.
%%
%%      Register a program with a lasp cluster.
%%
%%      This function will, given the path to a local file on each node,
%%      read, compile, parse_transform, and load the library into
%%      memory, with a copy at each replica, customized with a unique
%%      name, to ensure multiple replicas can run on the same physical
%%      node.
%%
%%      `Module' should be the name you want to refer to the program as
%%      when executing it using {@link execute/2}.
%%
%%      `File' is the path to the file on the node receiving the
%%      register request.
%%
%%      When the last argument is `preflist', the programs name will be
%%      hashed and installed on `?N' replicas; when the last argument is
%%      `global', a copy will be installed on all replicas.
%%
%%      Programs must implement the `lasp_program' behavior to be
%%      correct.
%%
-spec register(module(), file(), registration()) ->
    ok | {error, timeout} | error.
register(Module, File, preflist) ->
    {ok, ReqId} = lasp_register_fsm:register(Module, File),
    wait_for_reqid(ReqId, ?TIMEOUT);
register(Module, File, global) ->
    {ok, ReqId} = lasp_register_global_fsm:register(Module, File),
    wait_for_reqid(ReqId, ?TIMEOUT);
register(_Module, _File, _Registration) ->
    error.

%% @doc Execute an application.
%%
%%      Given a registered program using {@link register/3}, execute the
%%      program and receive it's result.
%%
%%      When executing a `preflist' program, contact `?N' replicas, wait
%%      for a majority and return the merge of the results.  When
%%      executing a `global' program, use the coverage facility of
%%      `riak_core' to compute a minimal covering set (or specify a `?R'
%%      value, and contact all nodes, merging the results.
%%
-spec execute(module(), registration()) ->
    {ok, result()} | {error, timeout} | error.
execute(Module, preflist) ->
    {ok, ReqId} = lasp_execute_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT);
execute(Module, global) ->
    {ok, ReqId} = lasp_execute_coverage_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT);
execute(_Module, _Registration) ->
    error.

%% @doc Notification of value change.
%%
%%      Notify a given partition at a given node that an object has
%%      changed for a particular reason.
%%
-spec process(module(), registration(), object(), reason(), idx(),
              node()) -> {ok, result()} | {error, timeout}.
process(Module, global, Object, Reason, Idx, Node) ->
    {ok, ReqId} = lasp_process_fsm:process(Module, Object, Reason, Idx, Node),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
-spec declare(type()) -> {ok, id()} | {error, timeout}.
declare(Type) ->
    Id = druuid:v4(),
    declare(Id, Type).

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
%%      Type is declared with the provided `Id'.
%%
-spec declare(id(), type()) -> {ok, id()} | {error, timeout}.
declare(Id, Type) ->
    {ok, ReqId} = lasp_declare_fsm:declare(Id, Type),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Update a dataflow variable.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor()) ->
    {ok, {value(), id()}} | {error, timeout}.
update(Id, Operation, Actor) ->
    {ok, ReqId} = lasp_update_fsm:update(Id, Operation, Actor),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, id()} | {error, timeout}.
bind(Id, Value) ->
    {ok, ReqId} = lasp_bind_fsm:bind(Id, Value),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Bind a dataflow variable to another dataflow variable.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind_to(id(), id()) -> {ok, id()} | {error, timeout}.
bind_to(Id, TheirId) ->
    {ok, ReqId} = lasp_bind_to_fsm:bind_to(Id, TheirId),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Bind a dataflow variable to the result of a function call.
%%
%%      Execute `Module:Function(Args)' and bind the result using {@link
%%      bind/2}.
%%
-spec bind(id(), module(), func(), args()) -> {ok, id()} | {error, timeout}.
bind(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

%% @doc Blocking read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound and
%%      then return the value.
%%
-spec read(id()) -> {ok, var()} | {error, timeout}.
read(Id) ->
    read(Id, {strict, undefined}).

%% @doc Blocking monotonic read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound, and
%%      is monotonically greater (as defined by the lattice) then the
%%      provided `Threshold' value.
%%
-spec read(id(), threshold()) -> {ok, var()} | {error, timeout}.
read(Id, Threshold) ->
    {ok, ReqId} = lasp_read_fsm:read(Id, Threshold),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Blocking monotonic read operation for a list of given dataflow
%%      variables.
%%
-spec read_any([{id(), threshold()}]) -> {ok, var()} | {error, timeout}.
read_any(Reads) ->
    ReqId = mk_reqid(),
    _ = [lasp_read_fsm:read(Id, Threshold, ReqId) || {Id, Threshold} <- Reads],
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id()) -> ok | {error, timeout}.
product(Left, Right, Product) ->
    {ok, ReqId} = lasp_product_fsm:product(Left, Right, Product),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id()) -> ok | {error, timeout}.
union(Left, Right, Union) ->
    {ok, ReqId} = lasp_union_fsm:union(Left, Right, Union),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id()) -> ok | {error, timeout}.
intersection(Left, Right, Intersection) ->
    {ok, ReqId} = lasp_intersection_fsm:intersection(Left, Right, Intersection),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id()) -> ok | {error, timeout}.
map(Id, Function, AccId) ->
    {ok, ReqId} = lasp_map_fsm:map(Id, Function, AccId),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id()) -> ok | {error, timeout}.
fold(Id, Function, AccId) ->
    {ok, ReqId} = lasp_fold_fsm:fold(Id, Function, AccId),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id()) -> ok | {error, timeout}.
filter(Id, Function, AccId) ->
    {ok, ReqId} = lasp_filter_fsm:filter(Id, Function, AccId),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Produce a value in a stream.
%%
%%      Given the `Id' of a declared value in a dataflow variable
%%      stream, bind `Value' to it.  Similar to {@link bind/2}.
%%
-spec produce(id(), value()) -> {ok, id()} | {error, timeout}.
produce(Id, Value) ->
    bind(Id, Value).

%% @doc Produce a value in a stream.
%%
%%      Given the `Id' of a declared variable in a dataflow variable
%%      stream, bind the result of `Module:Function(Args)' to it.
%%      Similar to {@link produce/2}.
%%
-spec produce(id(), module(), func(), args()) ->
    {ok, id()} | {error, timeout}.
produce(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

%% @doc Consume a value in the stream.
%%
%%      Given the `Id' of a declared variable in a dataflow stream, read
%%      the next value in the stream.
%%
-spec consume(id()) -> {ok, var()} | {error, timeout}.
consume(Id) ->
    read(Id, {strict, undefined}).

%% @doc Generate the next identifier in a stream.
%%
%%      Given `Id', return the next identifier needed to build a stream
%%      after this variable.
%%
-spec extend(id()) -> {ok, id()} | {error, timeout}.
extend(Id) ->
    {ok, ReqId} = lasp_next_fsm:next(Id),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args()) -> ok | {error, timeout}.
thread(Module, Function, Args) ->
    {ok, ReqId} = lasp_thread_fsm:thread(Module, Function, Args),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Pause execution until value requested.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id()) -> ok | {error, timeout}.
wait_needed(Id) ->
    wait_needed(Id, {strict, undefined}).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), threshold()) -> ok | {error, timeout}.
wait_needed(Id, Threshold) ->
    {ok, ReqId} = lasp_wait_needed_fsm:wait_needed(Id, Threshold),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Materialize all values in a stream and print to the log.
%%
%%      Meant primarily for debugging purposes.
%%
-spec get_stream(id()) -> stream().
get_stream(Stream) ->
    get_stream(Stream, []).

%% @doc Generate a preference list.
%%
%%      Given a `NVal', or replication factor, generate a preference
%%      list of primary replicas for a given `Param' and registered
%%      `VNode'.
%%
-spec preflist(non_neg_integer(), term(), atom()) ->
    riak_core_apl:preflist_ann().
preflist(NVal, Param, VNode) ->
    case application:get_env(lasp, single_partition_mode) of
        {ok, true} ->
            lager:info("Running in single partition mode!"),
            case riak_core_mochiglobal:get(primary_apl) of
                undefined ->
                    DocIdx = riak_core_util:chash_key({?BUCKET,
                                                       term_to_binary(Param)}),
                    Preflist = riak_core_apl:get_primary_apl(DocIdx,
                                                             NVal,
                                                             VNode),
                    ok = riak_core_mochiglobal:put(primary_apl, Preflist),
                    Preflist;
                Preflist ->
                    Preflist
            end;
        _ ->
            DocIdx = riak_core_util:chash_key({?BUCKET,
                                               term_to_binary(Param)}),
            riak_core_apl:get_primary_apl(DocIdx, NVal, VNode)
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Generate a request id.
%%
%%      Helper function; used to generate a unique request identifier.
%%
mk_reqid() ->
    erlang:phash2(erlang:now()).

%% @doc Wait for a response.
%%
%%      Helper function; given a `ReqId', wait for a message within
%%      `Timeout' seconds and return the result.
%%
wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Val} ->
            {ok, Val}
    after Timeout ->
        {error, timeout}
    end.

%% @doc Materialize all values in a stream and print to the log.
%%
%%      Meant primarily for debugging purposes. See {@link
%%      get_stream/1}.
%%
get_stream(Head, Output) ->
    lager:info("About to consume: ~p", [Head]),
    case consume(Head) of
        {ok, {_, _, nil, _}} ->
            lager:info("Received: ~p", [undefined]),
            Output;
        {ok, {_, _, Value, Next}} ->
            lager:info("Received: ~p", [Value]),
            get_stream(Next, lists:append(Output, [Value]))
    end.
