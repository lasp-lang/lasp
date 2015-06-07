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

-module(lasp_riak_core_broadcast_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_distribution_backend).
-behaviour(riak_core_broadcast_handler).

%% API
-export([start_link/0,
         start_link/1]).

%% lasp_distribution_backend callbacks
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

%% riak_core_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1,
         exchange/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store()}).

%% Broadcast record.
-record(broadcast, {id :: id(), type :: type(), value :: value()}).

%% Definitions for the bind/read fun abstraction.
-define(BIND, fun(_AccId, _AccValue, _Store) ->
                ?CORE:bind(_AccId, _AccValue, _Store)
              end).

-define(READ, fun(_Id, _Threshold) ->
                ?CORE:read(_Id, _Threshold, Store)
              end).

-define(BLOCKING, fun() -> {noreply, State} end).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% riak_core_broadcast_handler callbacks
%%%===================================================================

%% @todo doc
%% @todo spec
broadcast_data(#broadcast{id=Id, type=Type, value=Value}=Msg) ->
    lager:warning("id: ~p, msg: ~p", [Id, Msg]),
    {{Id, Type, Value}, {Id, Type, Value}}.

%% @todo doc
%% @todo spec
merge({Id, Type, Value}, {Id, Type, Value}) ->
    lager:warning("id: ~p, type: ~p value: ~p", [Id, Type, Value]),

    case is_stale({Id, Type, Value}) of
        true ->
            false;
        false ->
            {ok, _} = local_bind(Id, Type, Value),
            true
    end.

%% @todo doc
%% @todo spec
is_stale({Id, Type, Value}) ->
    lager:warning("id: ~p, type: ~p value: ~p", [Id, Type, Value]),

    Result = case read(Id, undefined) of
        {ok, {_I, T, V}} ->
            not lasp_lattice:is_lattice_strict_inflation(T, V, Value);
        _ ->
            %% We don't know about the value yet; not stale.
            false
    end,

    lager:warning("returning: ~p", [Result]),

    Result.

%% @todo doc
%% @todo spec
graft(Id) ->
    lager:warning("id: ~p", [Id]),
    {error, not_implemented}.

%% @todo doc
%% @todo spec
exchange(Peer) ->
    lager:warning("peer: ~p", [Peer]),
    {error, not_implemented}.

%%%===================================================================
%%% lasp_distribution_backend callbacks
%%%===================================================================

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.  Type is declared with the provided `Id'.
%%
-spec declare(id(), type()) -> {ok, id()}.
declare(Id, Type) ->
    {ok, Id} = gen_server:call(?MODULE, {declare, Id, Type}, infinity),
    % broadcast({Id, Type, undefined}),
    {ok, Id}.

%% @doc Update a dataflow variable.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor()) -> {ok, {value(), id()}} | error().
update(Id, Operation, Actor) ->
    gen_server:call(?MODULE, {update, Id, Operation, Actor}, infinity).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, var()}.
bind(Id, Value) ->
    {ok, Result} = gen_server:call(?MODULE, {bind, Id, Value}, infinity),
    broadcast(Result),
    {ok, Result}.

%% @doc Bind a dataflow variable to another dataflow variable.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind_to(id(), id()) -> {ok, id()} | error().
bind_to(Id, TheirId) ->
    gen_server:call(?MODULE, {bind_to, Id, TheirId}, infinity).

%% @doc Blocking monotonic read operation for a given dataflow variable.
%%
%%      Block until the variable identified by `Id' has been bound, and
%%      is monotonically greater (as defined by the lattice) then the
%%      provided `Threshold' value.
%%
-spec read(id(), threshold()) -> {ok, var()} | error().
read(Id, Threshold) ->
    gen_server:call(?MODULE, {read, Id, Threshold}, infinity).

%% @doc Blocking monotonic read operation for a list of given dataflow
%%      variables.
%%
-spec read_any([{id(), threshold()}]) -> {ok, var()} | error().
read_any(Reads) ->
    Self = self(),
    lists:foreach(fun({Id, Threshold}) ->
                spawn_link(fun() ->
                        Self ! read(Id, Threshold)
                    end)
        end, Reads),
    receive
        X ->
            X
    end.

%% @doc Compute the cartesian product of two sets.
%%
%%      Computes the cartestian product of two sets and bind the result
%%      to a third.
%%
-spec product(id(), id(), id()) -> ok | error().
product(Left, Right, Product) ->
    gen_server:call(?MODULE, {product, Left, Right, Product}, infinity).

%% @doc Compute the union of two sets.
%%
%%      Computes the union of two sets and bind the result
%%      to a third.
%%
-spec union(id(), id(), id()) -> ok | error().
union(Left, Right, Union) ->
    gen_server:call(?MODULE, {union, Left, Right, Union}, infinity).

%% @doc Compute the intersection of two sets.
%%
%%      Computes the intersection of two sets and bind the result
%%      to a third.
%%
-spec intersection(id(), id(), id()) -> ok | error().
intersection(Left, Right, Intersection) ->
    gen_server:call(?MODULE, {intersection, Left, Right, Intersection}, infinity).

%% @doc Map values from one lattice into another.
%%
%%      Applies the given `Function' as a map over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec map(id(), function(), id()) -> ok | error().
map(Id, Function, AccId) ->
    gen_server:call(?MODULE, {map, Id, Function, AccId}, infinity).

%% @doc Fold values from one lattice into another.
%%
%%      Applies the given `Function' as a fold over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec fold(id(), function(), id()) -> ok | error().
fold(Id, Function, AccId) ->
    gen_server:call(?MODULE, {fold, Id, Function, AccId}, infinity).

%% @doc Filter values from one lattice into another.
%%
%%      Applies the given `Function' as a filter over the items in `Id',
%%      placing the result in `AccId', both of which need to be declared
%%      variables.
%%
-spec filter(id(), function(), id()) -> ok | error().
filter(Id, Function, AccId) ->
    gen_server:call(?MODULE, {filter, Id, Function, AccId}, infinity).

%% @doc Spawn a function.
%%
%%      Spawn a process executing `Module:Function(Args)'.
%%
-spec thread(module(), func(), args()) -> ok | error().
thread(Module, Function, Args) ->
    gen_server:call(?MODULE, {thread, Module, Function, Args}, infinity).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), threshold()) -> ok | error().
wait_needed(Id, Threshold) ->
    gen_server:call(?MODULE, {wait_needed, Id, Threshold}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    {ok, Store} = ?CORE:start(node()),
    {ok, #state{store=Store}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) -> {reply, term(), #state{}}.
handle_call({declare, Id, Type}, _From, #state{store=Store}=State) ->
    {ok, Id} = ?CORE:declare(Id, Type, Store),
    {reply, {ok, Id}, State};
handle_call({bind, Id, Value}, _From, #state{store=Store}=State) ->
    Result = ?CORE:bind(Id, Value, Store),
    {reply, Result, State};
handle_call({bind_to, Id, DVId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:bind_to(Id, DVId, Store, ?BIND, ?READ),
    {reply, ok, State};
handle_call({update, Id, Operation, Actor}, _From, #state{store=Store}=State) ->
    {ok, Result} = ?CORE:update(Id, Operation, Actor, Store),
    {reply, {ok, Result}, State};
handle_call({thread, Module, Function, Args}, _From, #state{store=Store}=State) ->
    ok = ?CORE:thread(Module, Function, Args, Store),
    {reply, ok, State};
handle_call({wait_needed, Id, Threshold}, From, #state{store=Store}=State) ->
    ReplyFun = fun(ReadThreshold) ->
                        {reply, {ok, ReadThreshold}, State}
               end,
    ?CORE:wait_needed(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);
handle_call({read, Id, Threshold}, From, #state{store=Store}=State) ->
    %% @todo Normalize this ReplyFun in the future.
    ReplyFun = fun({_Id, Type, Value}) ->
                    {reply, {ok, {_Id, Type, Value}}, State};
                  ({error, Error}) ->
                    {reply, {error, Error}, State}
               end,
    ?CORE:read(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);
handle_call({filter, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:filter(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};
handle_call({product, Left, Right, Product}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:product(Left, Right, Product, Store, ?BIND, ?READ, ?READ),
    {reply, ok, State};
handle_call({intersection, Left, Right, Intersection}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:intersection(Left, Right, Intersection, Store, ?BIND, ?READ, ?READ),
    {reply, ok, State};
handle_call({union, Left, Right, Union}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:union(Left, Right, Union, Store, ?BIND, ?READ, ?READ),
    {reply, ok, State};
handle_call({map, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:map(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};
handle_call({fold, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:fold(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
broadcast({Id, Type, Value}) ->
    Broadcast = #broadcast{id=Id, type=Type, value=Value},
    riak_core_broadcast:broadcast(Broadcast, ?MODULE).

%% @private
local_bind(Id, Type, Value) ->
    lager:warning("id: ~p, type: ~p value: ~p", [Id, Type, Value]),

    case gen_server:call(?MODULE, {bind, Id, Value}, infinity) of
        {error, not_found} ->
            lager:warning("not found; declaring!"),
            {ok, Id} = gen_server:call(?MODULE, {declare, Id, Type}, infinity),
            local_bind(Id, Type, Value);
        {ok, X} ->
           {ok, X}
    end.
