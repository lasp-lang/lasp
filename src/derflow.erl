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

-module(derflow).

-include("derflow.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([declare/0,
         declare/1,
         bind/2,
         bind/4,
         read/1,
         read/2,
         select/3,
         produce/2,
         produce/4,
         consume/1,
         extend/1,
         is_det/1,
         wait_needed/1,
         spawn_mon/4,
         thread/3,
         preflist/3,
         get_stream/1,
         register/3,
         execute/2,
         mk_reqid/0]).

%% Public API

%% @doc Register an application.
%%
%%      Register a program with a derflow cluster.
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
%%      Programs must implement the `derflow_program' behavior to be
%%      correct.
%%
-spec register(module(), file(), registration()) -> ok | error.
register(Module, File, preflist) ->
    {ok, ReqId} = derflow_register_fsm:register(Module, File),
    wait_for_reqid(ReqId, ?TIMEOUT);
register(Module, File, global) ->
    {ok, ReqId} = derflow_register_global_fsm:register(Module, File),
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
-spec execute(module(), registration()) -> {ok, result()} | error.
execute(Module, preflist) ->
    {ok, ReqId} = derflow_execute_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT);
execute(Module, global) ->
    {ok, ReqId} = derflow_execute_coverage_fsm:execute(Module),
    wait_for_reqid(ReqId, ?TIMEOUT);
execute(_Module, _Registration) ->
    error.

%% @doc Declare a new single-assignment dataflow variable.
-spec declare() -> {ok, id()}.
declare() ->
    declare(undefined).

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.
%%
-spec declare(type()) -> {ok, id()}.
declare(Type) ->
    derflow_vnode:declare(druuid:v4(), Type).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, id()} | error.
bind(Id, Value) ->
    derflow_vnode:bind(Id, Value).

%% @doc Bind a dataflow variable to the result of a function call.
%%
%%      Execute `Module:Function(Args)' and bind the result using {@link
%%      bind/2}.
%%
-spec bind(id(), module(), func(), args()) -> {ok, id()} | error.
bind(Id, Module, Function, Args) ->
    bind(Id, Module:Function(Args)).

%% @doc Blocking read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound and
%%      then return the value.
%%
-spec read(id()) -> {ok, type(), value(), id()}.
read(Id) ->
    derflow_vnode:read(Id).

%% @doc Blocking monotonic read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound, and
%%      is monotonically greater (as defined by the lattice) then the
%%      provided `Threshold' value.
%%
-spec read(id(), threshold()) -> {ok, type(), value(), id()}.
read(Id, Threshold) ->
    derflow_vnode:read(Id, Threshold).

%% @TODO: write documentation.
%% @TODO: write specification.
select(Id, Function, AccId) ->
    derflow_vnode:select(Id, Function, AccId).

%% @doc Produce a value in a stream.
%%
%%      Given the `Id' of a declared value in a dataflow variable
%%      stream, bind `Value' to it.  Similar to {@link bind/2}.
%%
-spec produce(id(), value()) -> {ok, id()}.
produce(Id, Value) ->
    derflow_vnode:bind(Id, Value).

%% @doc Produce a value in a stream.
%%
%%      Given the `Id' of a declared variable in a dataflow variable
%%      stream, bind the result of `Module:Function(Args)' to it.
%%      Similar to {@link produce/2}.
%%
-spec produce(id(), module(), func(), args()) -> {ok, id()}.
produce(Id, Module, Function, Args) ->
    derflow_vnode:bind(Id, Module:Function(Args)).

%% @doc Consume a value in the stream.
%%
%%      Given the `Id' of a declared variable in a dataflow stream, read
%%      the next value in the stream.
%%
-spec consume(id()) -> {ok, type(), value(), id()}.
consume(Id) ->
    derflow_vnode:read(Id).

%% @doc Generate the next identifier in a stream.
%%
%%      Given `Id', return the next identifier needed to build a stream
%%      after this variable.
%%
-spec extend(id()) -> id().
extend(Id) ->
    derflow_vnode:next(Id).

%% @doc Inspect the bind status of a variable.
%%
%%      Return the bound status of `Id'.
%%
%%      Operator introduces non-determinism if a choice is made using
%%      the result.
%%
-spec is_det(id()) -> bound().
is_det(Id) ->
    derflow_vnode:is_det(Id).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args()) -> {ok, pid()}.
thread(Module, Function, Args) ->
    derflow_vnode:thread(Module, Function, Args).

%% @doc Pause execution until value requested.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id()) -> ok.
wait_needed(Id) ->
    derflow_vnode:wait_needed(Id).

%% @doc Spawn monitor.
%%
%%      Spawn a process and register the process with a given derflow
%%      supervisor process.
%%
-spec spawn_mon(supervisor(), module(), func(), args()) -> ok.
spawn_mon(Supervisor, Module, Function, Args) ->
    {ok, Pid} = thread(Module, Function, Args),
    Supervisor ! {'SUPERVISE', Pid, Module, Function, Args},
    ok.

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
%% @TODO: write specification.
preflist(NVal, Param, VNode) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Param)}),
    riak_core_apl:get_primary_apl(DocIdx, NVal, VNode).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Generate a request id.
%%
%%      Helper function; used to generate a unique request identifier.
%%
%% @TODO: write specification.
mk_reqid() ->
    erlang:phash2(erlang:now()).

%% @doc Wait for a response.
%%
%%      Helper function; given a `ReqId', wait for a message within
%%      `Timeout' seconds and return the result.
%%
%% @TODO: write specification.
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
%%      Meant primarily for debugging purposes.
%%
get_stream(Head, Output) ->
    lager:info("About to consume: ~p", [Head]),
    case consume(Head) of
        {ok, _, undefined, _} ->
            lager:info("Received: ~p", [undefined]),
            Output;
        {ok, _, Value, Next} ->
            lager:info("Received: ~p", [Value]),
            get_stream(Next, lists:append(Output, [Value]))
    end.
