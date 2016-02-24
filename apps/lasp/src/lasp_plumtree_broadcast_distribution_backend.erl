%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_plumtree_broadcast_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_distribution_backend).
-behaviour(plumtree_broadcast_handler).

%% API
-export([start_link/0,
         start_link/1]).

%% lasp_distribution_backend callbacks
-export([declare/2,
         declare_dynamic/2,
         query/1,
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

%% plumtree_broadcast_handler callbacks
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
-record(state, {store :: store(),
                actor :: binary(),
                counter :: non_neg_integer()}).

%% Broadcast record.
-record(broadcast, {id :: id(),
                    type :: type(),
                    clock :: riak_dt_vclock:vclock(),
                    metadata :: metadata(),
                    value :: value()}).

%% Definitions for the bind/read fun abstraction.

-define(BIND, fun(_AccId, _AccValue, _Store) ->
                ?CORE:bind(_AccId, _AccValue, _Store)
              end).

-define(READ, fun(_Id, _Threshold) ->
                ?CORE:read(_Id, _Threshold, Store)
              end).

-define(BLOCKING, fun() -> {noreply, State} end).

%% Metadata mutation macros.

-define(CLOCK_INIT, fun(Metadata) ->
            VClock = riak_dt_vclock:increment(Actor, riak_dt_vclock:fresh()),
            orddict:store(clock, VClock, Metadata)
    end).

-define(CLOCK_INCR, fun(Metadata) ->
            Clock = orddict:fetch(clock, Metadata),
            VClock = riak_dt_vclock:increment(Actor, Clock),
            orddict:store(clock, VClock, Metadata)
    end).

-define(CLOCK_MERG, fun(Metadata) ->
            %% Incoming request has to have a clock, given it's coming
            %% in the broadcast path.
            TheirClock = orddict:fetch(clock, Metadata0),

            %% We may not have a clock yet, if we are first initializing
            %% an object.
            OurClock = case orddict:find(clock, Metadata) of
                {ok, Clock} ->
                    Clock;
                _ ->
                    riak_dt_vclock:fresh()
            end,

            %% Merge the clocks.
            Merged = riak_dt_vclock:merge([TheirClock, OurClock]),
            orddict:store(clock, Merged, Metadata)
    end).

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
%%% plumtree_broadcast_handler callbacks
%%%===================================================================

-type clock() :: riak_dt_vclock:vclock().

-type broadcast_message() :: #broadcast{}.
-type broadcast_id() :: id().
-type broadcast_clock() :: clock().
-type broadcast_payload() :: {id(), type(), metadata(), value()}.

%% @doc Returns from the broadcast message the identifier and the payload.
-spec broadcast_data(broadcast_message()) ->
    {{broadcast_id(), broadcast_clock()}, broadcast_payload()}.
broadcast_data(#broadcast{id=Id, type=Type, clock=Clock,
                          metadata=Metadata, value=Value}) ->
    {{Id, Clock}, {Id, Type, Metadata, Value}}.

%% @doc Perform a merge of an incoming object with an object in the
%%      local datastore, as long as we haven't seen a more recent clock
%%      for the same object.
-spec merge({broadcast_id(), broadcast_clock()}, broadcast_payload()) ->
    boolean().
merge({Id, Clock}, {Id, Type, Metadata, Value}) ->
    case is_stale({Id, Clock}) of
        true ->
            false;
        false ->
            {ok, _} = local_bind(Id, Type, Metadata, Value),
            true
    end.

%% @doc Use the clock on the object to determine if this message is
%%      stale or not.
-spec is_stale({broadcast_id(), broadcast_clock()}) -> boolean().
is_stale({Id, Clock}) ->
    gen_server:call(?MODULE, {is_stale, Id, Clock}, infinity).

%% @doc Given a message identifier and a clock, return a given message.
-spec graft({broadcast_id(), broadcast_clock()}) ->
    stale | {ok, broadcast_payload()} | {error, term()}.
graft({Id, Clock}) ->
    gen_server:call(?MODULE, {graft, Id, Clock}, infinity).

%% @doc Naive anti-entropy mechanism; re-broadcast all messages.
-spec exchange(node()) -> {ok, pid()}.
exchange(_Peer) ->
    gen_server:call(?MODULE, exchange, infinity).

%%%===================================================================
%%% lasp_distribution_backend callbacks
%%%===================================================================

%% @doc Declare a new dataflow variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.  Type is declared with the provided `Id'.
%%
-spec declare(id(), type()) -> {ok, var()}.
declare(Id, Type) ->
    {ok, Variable} = gen_server:call(?MODULE, {declare, Id, Type}, infinity),
    broadcast(Variable),
    {ok, Variable}.

%% @doc Declare a new dynamic variable of a given type.
%%
%%      Valid values for `Type' are any of lattices supporting the
%%      `riak_dt' behavior.  Type is declared with the provided `Id'.
%%
-spec declare_dynamic(id(), type()) -> {ok, var()}.
declare_dynamic(Id, Type) ->
    {ok, Variable} = gen_server:call(?MODULE,
                                     {declare_dynamic, Id, Type}, infinity),
    broadcast(Variable),
    {ok, Variable}.

%% @doc Read the current value of a CRDT.
%%
%%      Given a `Id', read the current value, compute the `query'
%%      operation for the value, and return it to the user.
%%
-spec query(id()) -> {ok, term()} | error().
query(Id) ->
    gen_server:call(?MODULE, {query, Id}, infinity).

%% @doc Update a dataflow variable.
%%
%%      Read the given `Id' and update it given the provided
%%      `Operation', which should be valid for the type of CRDT stored
%%      at the given `Id'.
%%
-spec update(id(), operation(), actor()) -> {ok, var()} | error().
update(Id, Operation, Actor) ->
    {ok, {Id, Type, Metadata, Value}} = gen_server:call(?MODULE,
                                                        {update, Id, Operation, Actor},
                                                        infinity),
    BroadcastState = case Value of
                         {delta, DeltaState, _MergedState} ->
                             {delta, DeltaState};
                         _ ->
                             Value
                     end,
    ReturnState = case Value of
                      {delta, _DeltaState, MergedState} ->
                          MergedState;
                      _ ->
                          Value
                  end,
    case orddict:find(dynamic, Metadata) of
        {ok, true} ->
            %% Ignore: this is a dynamic variable.
            ok;
        _ ->
            broadcast({Id, Type, Metadata, BroadcastState})
    end,
    {ok, {Id, Type, Metadata, ReturnState}}.

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, var()}.
bind(Id, Value0) ->
    {ok, {Id, Type, Metadata, Value}} = gen_server:call(?MODULE,
                                                        {bind, Id, Value0},
                                                        infinity),
    BroadcastState = case Value of
                         {delta, DeltaState, _MergedState} ->
                             {delta, DeltaState};
                         _ ->
                             Value
                     end,
    ReturnState = case Value of
                      {delta, _DeltaState, MergedState} ->
                          MergedState;
                      _ ->
                          Value
                  end,
    case orddict:find(dynamic, Metadata) of
        {ok, true} ->
            %% Ignore: this is a dynamic variable.
            ok;
        _ ->
            broadcast({Id, Type, Metadata, BroadcastState})
    end,
    {ok, {Id, Type, Metadata, ReturnState}}.

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
    gen_server:call(?MODULE,
                    {intersection, Left, Right, Intersection},
                    infinity).

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
    {ok, Actor} = lasp_unique:unique(),
    Counter = 0,
    Identifier = node(),
    {ok, Store} = case ?CORE:start(Identifier) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            lager:error("Failed to initialize backend: ~p", [Reason]),
            {error, Reason}
    end,
    {ok, #state{actor=Actor, counter=Counter, store=Store}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% Local declare operation, which will need to initialize metadata and
%% broadcast the value to the remote nodes.
handle_call({declare, Id, Type}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    Result = ?CORE:declare(Id, Type, ?CLOCK_INIT, Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Incoming bind request, where we do not have information about the
%% variable yet.  In this case, take the remote metadata, if we don't
%% have local metadata.
handle_call({declare, Id, IncomingMetadata, Type}, _From,
            #state{store=Store, counter=Counter}=State) ->
    Metadata0 = case get(Id, Store) of
        {ok, {_, _, Metadata, _}} ->
            Metadata;
        {error, _Error} ->
            IncomingMetadata
    end,
    Result = ?CORE:declare(Id, Type, ?CLOCK_MERG, Metadata0, Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Issue a local dynamic declare operation, which should initialize
%% metadata and result in the broadcast operation.
handle_call({declare_dynamic, Id, Type}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    Result = ?CORE:declare_dynamic(Id, Type, ?CLOCK_INIT, Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Local query operation.
%% Return the value of a value in the datastore to the user.
handle_call({query, Id}, _From, #state{store=Store}=State) ->
    {ok, Value} = ?CORE:query(Id, Store),
    {reply, {ok, Value}, State};

%% Issue a local bind, which should increment the local clock and
%% broadcast the result.
handle_call({bind, Id, Value}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    Result = ?CORE:bind(Id, Value, ?CLOCK_INCR, Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Incoming bind operation; merge incoming clock with the remote clock.
%%
%% Note: we don't need to rebroadcast, because if the value resulting
%% from the merge changes and we issue re-bind, it should trigger the
%% clock to be incremented again and a subsequent broadcast.  However,
%% this could change if the other module is later refactored.
handle_call({bind, Id, Metadata0, Value}, _From,
            #state{store=Store, counter=Counter}=State) ->
    Result = ?CORE:bind(Id, Value, ?CLOCK_MERG, Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Bind two variables together.
handle_call({bind_to, Id, DVId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:bind_to(Id, DVId, Store, ?BIND, ?READ),
    {reply, ok, State};

%% Perform an update, and ensure that we bump the logical clock as we
%% perform the update.
handle_call({update, Id, Operation, Actor}, _From,
            #state{store=Store, counter=Counter}=State) ->
    {ok, Result} = ?CORE:update(Id, Operation, Actor, ?CLOCK_INCR, Store),
    {reply, {ok, Result}, State#state{counter=increment_counter(Counter)}};

%% Spawn a function.
handle_call({thread, Module, Function, Args},
            _From,
            #state{store=Store}=State) ->
    ok = ?CORE:thread(Module, Function, Args, Store),
    {reply, ok, State};

%% Block, enforcing lazy evaluation of the provided function.
handle_call({wait_needed, Id, Threshold}, From, #state{store=Store}=State) ->
    ReplyFun = fun(ReadThreshold) -> {reply, {ok, ReadThreshold}, State} end,
    ?CORE:wait_needed(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);

%% Attempt to read a value.
handle_call({read, Id, Threshold}, From, #state{store=Store}=State) ->
    ReplyFun = fun({_Id, Type, Metadata, Value}) ->
                    {reply, {ok, {_Id, Type, Metadata, Value}}, State};
                  ({error, Error}) ->
                    {reply, {error, Error}, State}
               end,
    ?CORE:read(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);

%% Spawn a process to perform a filter.
handle_call({filter, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:filter(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};

%% Spawn a process to compute a product.
handle_call({product, Left, Right, Product},
            _From,
            #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:product(Left, Right, Product, Store, ?BIND,
                               ?READ, ?READ),
    {reply, ok, State};

%% Spawn a process to compute the intersection.
handle_call({intersection, Left, Right, Intersection},
            _From,
            #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:intersection(Left, Right, Intersection, Store,
                                    ?BIND, ?READ, ?READ),
    {reply, ok, State};

%% Spawn a process to compute the union.
handle_call({union, Left, Right, Union}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:union(Left, Right, Union, Store, ?BIND, ?READ,
                             ?READ),
    {reply, ok, State};

%% Spawn a process to perform a map.
handle_call({map, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:map(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};

%% Spawn a process to perform a fold.
handle_call({fold, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    {ok, _Pid} = ?CORE:fold(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};

%% Graft part of the tree back.  If the value that's being asked for is
%% stale: that is, it has an earlier vector clock than one that's stored
%% locally, ignore and return the stale marker, preventing tree repair
%% given we've shown we already have some connection to retrieve data
%% for that node.  If not, return the the value and repair the tree.
handle_call({graft, Id, TheirClock}, _From, #state{store=Store}=State) ->
    Result = case get(Id, Store) of
        {ok, {Id, Type, Metadata, Value}} ->
            OurClock = orddict:fetch(clock, Metadata),
            case riak_dt_vclock:equal(TheirClock, OurClock) of
                true ->
                    {ok, {Id, Type, Metadata, Value}};
                false ->
                    stale
            end;
        {error, Error} ->
            {error, Error}
    end,
    {reply, Result, State};

%% Given a message identifer, return stale if we've seen an object with
%% a vector clock that's greater than the provided one.
handle_call({is_stale, Id, TheirClock}, _From, #state{store=Store}=State) ->
    Result = case get(Id, Store) of
        {ok, {_, _, Metadata, _}} ->
            OurClock = orddict:fetch(clock, Metadata),
            riak_dt_vclock:descends(TheirClock, OurClock);
        {error, _Error} ->
            false
    end,
    {reply, Result, State};

%% Naive anti-entropy mechanism; periodically re-broadcast all messages.
handle_call(exchange, _From, #state{store=Store}=State) ->
    Function = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}, Acc0) ->
                    case orddict:find(dynamic, Metadata) of
                        {ok, true} ->
                            %% Ignore: this is a dynamic variable.
                            ok;
                        _ ->
                            broadcast({Id, Type, Metadata, Value})
                    end,
                    [{ok, {Id, Type, Metadata, Value}}|Acc0]
               end,
    Pid = spawn(fun() -> do(fold, [Store, Function, []]) end),
    {reply, {ok, Pid}, State};

%% @private
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
broadcast({Id, Type, Metadata, Value}) ->
    Clock = orddict:fetch(clock, Metadata),
    Broadcast = #broadcast{id=Id, clock=Clock, type=Type,
                           metadata=Metadata, value=Value},
    plumtree_broadcast:broadcast(Broadcast, ?MODULE).

%% @private
local_bind(Id, Type, Metadata, Value) ->
    case gen_server:call(?MODULE, {bind, Id, Metadata, Value}, infinity) of
        {error, not_found} ->
            {ok, _} = gen_server:call(?MODULE,
                                      {declare, Id, Metadata, Type},
                                      infinity),
            local_bind(Id, Type, Metadata, Value);
        {ok, X} ->
           {ok, X}
    end.

%% @private
-spec get(id(), store()) -> {ok, var()} | {error, term()}.
get(Id, _Store) when is_list(Id) ->
    %% This is only in here to satisfy dialyzer that the second clause
    %% can return {error, term()} given it does not think it can because
    %% of the use of higher-order functions.
    {error, unsupported};
get(Id, Store) ->
    ReplyFun = fun({_Id, Type, Metadata, Value}) ->
                    {ok, {_Id, Type, Metadata, Value}};
                  ({error, Error}) ->
                    {error, Error}
               end,
    BlockingFun = fun() ->
            {error, blocking}
    end,
    Threshold = undefined,
    ?CORE:read(Id, Threshold, Store, self(), ReplyFun, BlockingFun).

%% @private
increment_counter(Counter) ->
    Counter + 1.

-ifdef(TEST).

do(Function, Args) ->
    Backend = lasp_ets_storage_backend,
    erlang:apply(Backend, Function, Args).

-else.

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = application:get_env(?APP,
                                  storage_backend,
                                  lasp_dets_storage_backend),
    erlang:apply(Backend, Function, Args).

-endif.
