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

-module(lasp_default_broadcast_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(lasp_distribution_backend).
-behaviour(plumtree_broadcast_handler).

%% Administrative controls.
-export([reset/0]).

%% API
-export([start_link/0,
         start_link/1]).

%% lasp_distribution_backend callbacks
-export([declare/2,
         declare_dynamic/2,
         stream/2,
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

%% debug callbacks
-export([local_bind/4]).

%% broadcast interface
-export([broadcast/1]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store(),
                actor :: binary(),
                counter :: non_neg_integer(),
                gc_counter :: non_neg_integer()}).

%% Broadcast record.
-record(broadcast, {id :: id(),
                    type :: type(),
                    clock :: lasp_vclock:vclock(),
                    metadata :: metadata(),
                    value :: value()}).

-define(DELTA_GC_INTERVAL, 60000).
-define(PLUMTREE_MEMORY_INTERVAL, 10000).
-define(MEMORY_UTILIZATION_INTERVAL, 10000).

%% Definitions for the bind/read fun abstraction.

-define(BIND, fun(_AccId, _AccValue, _Store) ->
                ?CORE:bind(_AccId, _AccValue, _Store)
              end).

-define(WRITE, fun(_Store) ->
                 fun(_AccId, _AccValue) ->
                   {ok, _} = ?CORE:bind_var(_AccId, _AccValue, _Store)
                 end
               end).

-define(READ, fun(_Id, _Threshold) ->
                ?CORE:read_var(_Id, _Threshold, Store)
              end).

-define(BLOCKING, fun() -> {noreply, State} end).

%% Metadata mutation macros.

-define(CLOCK_INIT(BackendActor), fun(Metadata) ->
                                    VClock = lasp_vclock:increment(BackendActor, lasp_vclock:fresh()),
                                    orddict:store(clock, VClock, Metadata)
                                  end).

-define(CLOCK_INCR(BackendActor), fun(Metadata) ->
                                        Clock = orddict:fetch(clock, Metadata),
                                        VClock = lasp_vclock:increment(BackendActor, Clock),
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
                    lasp_vclock:fresh()
            end,

            %% Merge the clocks.
            Merged = lasp_vclock:merge([TheirClock, OurClock]),
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

-type clock() :: lasp_vclock:vclock().

-type broadcast_message() :: #broadcast{}.
-type broadcast_id() :: id().
-type broadcast_clock() :: clock().
-type broadcast_payload() :: {id(), type(), metadata(), value()}.

%% @doc Returns from the broadcast message the identifier and the payload.
-spec broadcast_data(broadcast_message()) ->
    {{broadcast_id(), broadcast_clock()}, broadcast_payload()}.
broadcast_data(#broadcast{id=Id,
                          type=Type,
                          clock=Clock,
                          metadata=Metadata,
                          value=Value}) ->
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
            %% Bind information.
            {ok, _} = ?MODULE:local_bind(Id, Type, Metadata, Value),
            true
    end;
merge(BroadcastId, Payload) ->
    lager:error("Incoming merge didn't match; broadcast_id: ~p payload: ~p", [BroadcastId, Payload]),
    false.

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

%% @doc Anti-entropy mechanism.
-spec exchange(node()) -> {ok, pid()}.
exchange(Peer) ->
    case lasp_config:get(mode, state_based) of
        delta_based ->
            %% Ignore the standard anti-entropy mechanism from plumtree.
            %%
            %% Spawn a process that terminates immediately, because the
            %% broadcast exchange timer tracks the number of in progress
            %% exchanges and bounds it by that limit.
            %%
            Pid = spawn_link(fun() -> ok end),
            {ok, Pid};
        state_based ->
            case broadcast_tree_mode() of
                true ->
                    %% Plumtree AAE.
                    gen_server:call(?MODULE, {exchange, Peer}, infinity);
                false ->
                    %% No AAE through this mechanism.
                    %%
                    %% Spawn a process that terminates immediately, because the
                    %% broadcast exchange timer tracks the number of in progress
                    %% exchanges and bounds it by that limit.
                    %%
                    Pid = spawn_link(fun() -> ok end),
                    {ok, Pid}
            end
    end.

%%%===================================================================
%%% lasp_distribution_backend callbacks
%%%===================================================================

%% @doc Declare a new dataflow variable of a given type.
-spec declare(id(), type()) -> {ok, var()}.
declare(Id, Type) ->
    case lasp_config:get(mode, state_based) of
        delta_based ->
            gen_server:call(?MODULE, {declare, Id, Type}, infinity);
        state_based ->
            {ok, Variable} = gen_server:call(?MODULE,
                                             {declare, Id, Type}, infinity),
            buffer_broadcast(Variable),
            {ok, Variable}
    end.

%% @doc Declare a new dynamic variable of a given type.
-spec declare_dynamic(id(), type()) -> {ok, var()}.
declare_dynamic(Id, Type) ->
    case lasp_config:get(mode, state_based) of
        delta_based ->
            gen_server:call(?MODULE, {declare_dynamic, Id, Type}, infinity);
        state_based ->
            {ok, Variable} = gen_server:call(?MODULE,
                                             {declare_dynamic, Id, Type},
                                             infinity),
            buffer_broadcast(Variable),
            {ok, Variable}
    end.

%% @doc Stream values out of the Lasp system; using the values from this
%%      stream can result in observable nondeterminism.
%%
stream(Id, Function) ->
    gen_server:call(?MODULE, {stream, Id, Function}, infinity).

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
    ReturnState = case orddict:find(dynamic, Metadata) of
                      {ok, true} ->
                          %% Ignore: this is a dynamic variable.
                          Value;
                      _ ->
                          case lasp_config:get(mode, state_based) of
                              delta_based  ->
                                  %% No broadcasting for the delta.
                                  Value;
                              state_based ->
                                  buffer_broadcast({Id, Type, Metadata, Value}),
                                  Value;
                              pure_op_based ->
                                  ok %% @todo
                          end
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
    ReturnState = case orddict:find(dynamic, Metadata) of
                      {ok, true} ->
                          %% Ignore: this is a dynamic variable.
                          Value;
                      _ ->
                          case lasp_config:get(mode, state_based) of
                              delta_based ->
                                  %% No broadcasting for the delta.
                                  Value;
                              state_based ->
                                  buffer_broadcast({Id, Type, Metadata, Value}),
                                  Value;
                              pure_op_based ->
                                  ok %% todo
                          end
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
%%% Administrative controls
%%%===================================================================

%% @doc Reset all Lasp application state.
-spec reset() -> ok.
reset() ->
    gen_server:call(?MODULE, reset, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Seed the process at initialization.
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),

    schedule_aae_synchronization(),
    schedule_delta_synchronization(),
    schedule_delta_garbage_collection(),

    {ok, Actor} = lasp_unique:unique(),

    Counter = 0,
    GCCounter = 0,
    Identifier = node(),

    {ok, Store} = case ?CORE:start(Identifier) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            _ = lager:error("Failed to initialize backend: ~p", [Reason]),
            {error, Reason}
    end,

    %% Schedule reports.
    schedule_plumtree_memory_report(),
    schedule_memory_utilization_report(),

    {ok, #state{actor=Actor,
                counter=Counter,
                store=Store,
                gc_counter=GCCounter}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% Reset all Lasp application state.
handle_call(reset, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("reset"),

    %% Terminate all Lasp processes.
    _ = lasp_process_sup:terminate(),

    %% Reset storage backend.
    ?CORE:storage_backend_reset(Store),

    {reply, ok, State};

%% Local declare operation, which will need to initialize metadata and
%% broadcast the value to the remote nodes.
handle_call({declare, Id, Type}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("declare/2"),

    Result = ?CORE:declare(Id, Type, ?CLOCK_INIT(Actor), Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Incoming bind request, where we do not have information about the
%% variable yet.  In this case, take the remote metadata, if we don't
%% have local metadata.
handle_call({declare, Id, IncomingMetadata, Type}, _From,
            #state{store=Store, counter=Counter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("declare/3"),

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
    lasp_marathon_simulations:log_message_queue_size("declare_dynamic"),

    Result = ?CORE:declare_dynamic(Id, Type, ?CLOCK_INIT(Actor), Store),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Stream values out of the Lasp system; using the values from this
%% stream can result in observable nondeterminism.
%%
handle_call({stream, Id, Function}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("stream"),

    {ok, _Pid} = ?CORE:stream(Id, Function, Store),
    {reply, ok, State};

%% Local query operation.
%% Return the value of a value in the datastore to the user.
handle_call({query, Id}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("query"),

    {ok, Value} = ?CORE:query(Id, Store),
    {reply, {ok, Value}, State};

%% Issue a local bind, which should increment the local clock and
%% broadcast the result.
handle_call({bind, Id, Value}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind/2"),

    Result0 = ?CORE:bind(Id, Value, ?CLOCK_INCR(Actor), Store),
    Result = declare_if_not_found(Result0, Id, State, ?CORE, bind,
                                  [Id, Value, ?CLOCK_INCR(Actor), Store]),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Incoming bind operation; merge incoming clock with the remote clock.
%%
%% Note: we don't need to rebroadcast, because if the value resulting
%% from the merge changes and we issue re-bind, it should trigger the
%% clock to be incremented again and a subsequent broadcast.  However,
%% this could change if the other module is later refactored.
handle_call({bind, Id, Metadata0, Value}, _From,
            #state{store=Store, counter=Counter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind/3"),

    Result0 = ?CORE:bind(Id, Value, ?CLOCK_MERG, Store),
    Result = declare_if_not_found(Result0, Id, State, ?CORE, bind,
                                  [Id, Value, ?CLOCK_MERG, Store]),
    {reply, Result, State#state{counter=increment_counter(Counter)}};

%% Bind two variables together.
handle_call({bind_to, Id, DVId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind_to"),

    {ok, _Pid} = ?CORE:bind_to(Id, DVId, Store, ?WRITE, ?READ),
    {reply, ok, State};

%% Perform an update, and ensure that we bump the logical clock as we
%% perform the update.
%%
%% The CRDT actor is used to distinguish actors *per-thread*, if
%% necessary, where the vclock is serialized *per-node*.
%%
handle_call({update, Id, Operation, CRDTActor}, _From,
            #state{store=Store, actor=Actor, counter=Counter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("update"),

    Result0 = ?CORE:update(Id, Operation, CRDTActor, ?CLOCK_INCR(Actor),
                           ?CLOCK_INIT(Actor), Store),
    {ok, Result} = declare_if_not_found(Result0, Id, State, ?CORE, update,
                                        [Id, Operation, Actor, ?CLOCK_INCR(Actor), Store]),
    {reply, {ok, Result}, State#state{counter=increment_counter(Counter)}};

%% Spawn a function.
handle_call({thread, Module, Function, Args},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("thread"),

    ok = ?CORE:thread(Module, Function, Args, Store),
    {reply, ok, State};

%% Block, enforcing lazy evaluation of the provided function.
handle_call({wait_needed, Id, Threshold}, From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("wait_needed"),

    ReplyFun = fun(ReadThreshold) -> {reply, {ok, ReadThreshold}, State} end,
    ?CORE:wait_needed(Id, Threshold, Store, From, ReplyFun, ?BLOCKING);

%% Attempt to read a value.
handle_call({read, Id, Threshold}, From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("read"),

    ReplyFun = fun({_Id, Type, Metadata, Value}) ->
                    {reply, {ok, {_Id, Type, Metadata, Value}}, State};
                  ({error, Error}) ->
                    {reply, {error, Error}, State}
               end,
    Value0 = ?CORE:read(Id, Threshold, Store, From, ReplyFun, ?BLOCKING),
    declare_if_not_found(Value0, Id, State, ?CORE, read,
                         [Id, Threshold, Store, From, ReplyFun, ?BLOCKING]);

%% Spawn a process to perform a filter.
handle_call({filter, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("filter"),

    {ok, _Pid} = ?CORE:filter(Id, Function, AccId, Store, ?WRITE, ?READ),
    {reply, ok, State};

%% Spawn a process to compute a product.
handle_call({product, Left, Right, Product},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("product"),

    {ok, _Pid} = ?CORE:product(Left, Right, Product, Store, ?WRITE,
                               ?READ, ?READ),
    {reply, ok, State};

%% Spawn a process to compute the intersection.
handle_call({intersection, Left, Right, Intersection},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("intersection"),

    {ok, _Pid} = ?CORE:intersection(Left, Right, Intersection, Store,
                                    ?WRITE, ?READ, ?READ),
    {reply, ok, State};

%% Spawn a process to compute the union.
handle_call({union, Left, Right, Union}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("union"),

    {ok, _Pid} = ?CORE:union(Left, Right, Union, Store, ?WRITE, ?READ,
                             ?READ),
    {reply, ok, State};

%% Spawn a process to perform a map.
handle_call({map, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("map"),

    {ok, _Pid} = ?CORE:map(Id, Function, AccId, Store, ?WRITE, ?READ),
    {reply, ok, State};

%% Spawn a process to perform a fold.
handle_call({fold, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("fold"),

    {ok, _Pid} = ?CORE:fold(Id, Function, AccId, Store, ?BIND, ?READ),
    {reply, ok, State};

%% Graft part of the tree back.  If the value that's being asked for is
%% stale: that is, it has an earlier vector clock than one that's stored
%% locally, ignore and return the stale marker, preventing tree repair
%% given we've shown we already have some connection to retrieve data
%% for that node.  If not, return the the value and repair the tree.
handle_call({graft, Id, TheirClock}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("graft"),

    Result = case get(Id, Store) of
        {ok, {Id, Type, Metadata, Value}} ->
            OurClock = orddict:fetch(clock, Metadata),
            case lasp_vclock:equal(TheirClock, OurClock) of
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
    lasp_marathon_simulations:log_message_queue_size("is_stale"),

    Result = case get(Id, Store) of
        {ok, {_, _, Metadata, _}} ->
            OurClock = orddict:fetch(clock, Metadata),
            Stale = lasp_vclock:descends(OurClock, TheirClock),
            Stale;
        {error, _Error} ->
            false
    end,
    {reply, Result, State};

%% Naive anti-entropy mechanism; pairwise state shipping.
handle_call({exchange, Peer}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("exchange"),

    Function = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}, Acc0) ->
                    case orddict:find(dynamic, Metadata) of
                        {ok, true} ->
                            %% Ignore: this is a dynamic variable.
                            ok;
                        _ ->
                            send({aae_send, node(), {Id, Type, Metadata, Value}}, Peer)
                    end,
                    [{ok, {Id, Type, Metadata, Value}}|Acc0]
               end,
    Pid = spawn_link(fun() -> do(fold, [Store, Function, []]) end),
    {reply, {ok, Pid}, State};

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% Anti-entropy mechanism for causal consistency of delta-CRDT;
%% periodically ship delta-interval or entire state.
handle_cast({delta_exchange, Peer, ObjectFilterFun},
            #state{store=Store, gc_counter=GCCounter}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_exchange"),

    lasp_logger:extended("Exchange starting for ~p", [Peer]),

    Function = fun({Id, #dv{value=Value, type=Type, metadata=Metadata,
                            delta_counter=Counter, delta_map=DeltaMap,
                            delta_ack_map=AckMap}},
                   Acc0) ->
                       case ObjectFilterFun(Id) of
                           true ->
                               Ack = case orddict:find(Peer, AckMap) of
                                         {ok, {Ack0, _GCed}} ->
                                             Ack0;
                                         error ->
                                             0
                                     end,
                               Min = case orddict:fetch_keys(DeltaMap) of
                                   [] ->
                                       0;
                                   Keys ->
                                       lists:min(Keys)
                               end,
                               Deltas = case orddict:is_empty(DeltaMap) orelse Min > Ack of
                                   true ->
                                       Value;
                                   false ->
                                       collect_deltas(Peer, Type, DeltaMap, Ack, Counter)
                               end,
                               send({delta_send, node(), {Id, Type, Metadata, Deltas}, Counter}, Peer),
                               [{ok, Id}|Acc0];
                           false ->
                               Acc0
                       end
               end,
    %% TODO: Should this be parallel?
    {ok, Result} = do(fold, [Store, Function, []]),
    lasp_logger:extended("Finished exchange peer: ~p; sent ~p objects", [Peer, length(Result)]),

    {noreply, State#state{gc_counter=increment_counter(GCCounter)}};

handle_cast({aae_send, From, {Id, Type, _Metadata, Value}},
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("aae_send"),

    ?CORE:receive_value(Store, {aae_send,
                                From,
                               {Id, Type, _Metadata, Value},
                               ?CLOCK_INCR(Actor),
                               ?CLOCK_INIT(Actor)}),

    %% Send back just the updated state for the object received.
    case client_server_mode() andalso i_am_server() andalso reactive_server() of
        true ->
            lager:info("~n~n---------------------------~n ShouldREACT ~p ~n~n---------------------------~n", [true]),
            ObjectFilterFun = fun(Id1) ->
                                      Id =:= Id1
                              end,
            init_aae_sync(From, ObjectFilterFun, Store);
        false ->
            lager:info("~n~n---------------------------~n ShouldREACT ~p ~n~n---------------------------~n", [false]),
            ok
    end,

    {noreply, State};

handle_cast({delta_send, From, {Id, Type, _Metadata, Deltas}, Counter},
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_send"),

    ?CORE:receive_delta(Store, {delta_send,
                                From,
                               {Id, Type, _Metadata, Deltas},
                               ?CLOCK_INCR(Actor),
                               ?CLOCK_INIT(Actor)}),

    %% Acknowledge message.
    send({delta_ack, node(), Id, Counter}, From),

    %% Send back just the updated state for the object received.
    case client_server_mode() andalso i_am_server() andalso reactive_server() of
        true ->
            ObjectFilterFun = fun(Id1) ->
                                      Id =:= Id1
                              end,
            init_delta_sync(From, ObjectFilterFun);
        false ->
            ok
    end,

    {noreply, State};

handle_cast({delta_ack, From, Id, Counter}, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_ack"),

    ?CORE:receive_delta(Store, {delta_ack, Id, From, Counter}),
    {noreply, State};

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(plumtree_memory_report, State) ->
    lasp_marathon_simulations:log_message_queue_size("plumtree_memory_report"),

    %% Log
    plumtree_memory_report(),

    %% Schedule report.
    schedule_plumtree_memory_report(),

    {noreply, State};

handle_info(memory_utilization_report, State) ->
    lasp_marathon_simulations:log_message_queue_size("memory_utilization_report"),

    %% Log
    memory_utilization_report(),

    %% Schedule report.
    schedule_memory_utilization_report(),

    {noreply, State};

handle_info(aae_sync, #state{store=Store} = State) ->
    lasp_marathon_simulations:log_message_queue_size("aae_sync"),

    lasp_logger:extended("Beginning AAE synchronization."),

    %% Get the active set from the membership protocol.
    {ok, Members} = membership(),

    %% Remove ourself.
    Peers = Members -- [node()],

    lasp_logger:extended("Beginning sync for peers: ~p", [Peers]),

    %% Ship buffered updates for the fanout value.
    lists:foreach(fun(Peer) -> init_aae_sync(Peer, Store) end, Peers),

    %% Schedule next synchronization.
    schedule_aae_synchronization(),

    {noreply, State};
handle_info(delta_sync, #state{}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_sync"),

    lasp_logger:extended("Beginning delta synchronization."),

    %% Get the active set from the membership protocol.
    {ok, Members} = membership(),

    PeerServiceManager = lasp_config:peer_service_manager(),
    lager:info("Manager is: ~p, Members are: ~p",
               [PeerServiceManager, Members]),

    %% Remove ourself and compute exchange peers.
    Peers = compute_exchange(without_me(Members)),

    lasp_logger:extended("Beginning sync for peers: ~p", [Peers]),

    %% Ship buffered updates for the fanout value.
    WithoutConvergenceFun = fun(Id) ->
                              Id =/= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, WithoutConvergenceFun) end,
                  Peers),

    %% Synchronize convergence structure.
    WithConvergenceFun = fun(Id) ->
                              Id =:= ?SIM_STATUS_STRUCTURE
                      end,
    lists:foreach(fun(Peer) ->
                          init_delta_sync(Peer, WithConvergenceFun) end,
                  without_me(Members)),

    %% Schedule next synchronization.
    schedule_delta_synchronization(),

    {noreply, State#state{}};
handle_info(delta_gc, #state{gc_counter=GCCounter0, store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("delta_gc"),

    MaxGCCounter = lasp_config:get(delta_mode_max_gc_counter, ?MAX_GC_COUNTER),
    Function =
        fun({Id, #dv{delta_map=DeltaMap0, delta_ack_map=AckMap0,
                     delta_counter=Counter}=_Object},
            Acc0) ->
                case orddict:is_empty(DeltaMap0) of
                    true ->
                        Acc0;
                    false ->
                        %% Remove disconnected or slow node() entries from the AckMap.
                        %% disconnected or slow: seen the previous GC & no change
                        RemovedAckMap0 =
                            orddict:filter(fun(_Node, {Ack, GCed}) ->
                                                   (GCed == false) orelse (Ack == Counter)
                                           end, AckMap0),
                        case orddict:size(RemovedAckMap0) of
                            0 ->
                                Acc0;
                            _ ->
                                %% Mark remained entries as seen this GC.
                                RemovedAckMap =
                                    orddict:fold(
                                      fun(Node, {Ack, _GCed}, RemovedAckMap1) ->
                                              orddict:store(Node, {Ack, true},
                                                            RemovedAckMap1)
                                      end, orddict:new(), RemovedAckMap0),
                                %% Collect garbage deltas.
                                MinAck =
                                    lists:min([Ack || {_Node, {Ack, _GCed}} <- RemovedAckMap]),
                                %% size() should be bigger than 0.
                                MinAck1 =
                                    case orddict:size(DeltaMap0) of
                                        1 ->
                                            MinAck;
                                        _ ->
                                            Counters = orddict:fetch_keys(DeltaMap0),
                                            NotCollectable =
                                                (MinAck > lists:nth(1, Counters)) andalso
                                                    (MinAck < lists:nth(2, Counters)),
                                            case NotCollectable of
                                                true ->
                                                    lists:nth(1, Counters);
                                                false ->
                                                    MinAck
                                            end
                                    end,
                                DeltaMap = orddict:filter(fun(Counter0, _Delta) ->
                                                                  Counter0 >= MinAck1
                                                          end, DeltaMap0),
                                [{ok, Id, DeltaMap, RemovedAckMap}|Acc0]
                        end
                end
        end,
    GCCounter = case GCCounter0 == MaxGCCounter of
        false ->
            GCCounter0;
        true ->
            spawn(
                fun() ->
                        {ok, Results} = do(fold, [Store, Function, []]),
                        lists:foreach(
                          fun({ok, Id, DeltaMap, RemovedAckMap}) ->
                                  case do(get, [Store, Id]) of
                                      {ok, Object} ->
                                          _ = do(put,
                                                 [Store, Id,
                                                  Object#dv{delta_map=DeltaMap,
                                                            delta_ack_map=RemovedAckMap}]),
                                          ok;
                                      _ ->
                                          ok
                                  end
                          end, Results)
                end),
            0
    end,

    %% Schedule next GC and reset counter.
    schedule_delta_garbage_collection(),

    {noreply, State#state{gc_counter=GCCounter}};

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
broadcast({Id, Type, Metadata, Value}) ->
    case broadcast_tree_mode() of
        true ->
            Clock = orddict:fetch(clock, Metadata),
            Broadcast = #broadcast{id=Id,
                                   clock=Clock,
                                   type=Type,
                                   metadata=Metadata,
                                   value=Value},
            ok = plumtree_broadcast:broadcast(Broadcast, ?MODULE),
            ok;
        false ->
            ok
    end.

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

%% @private
collect_deltas(Destination, Type, DeltaMap, Min0, Max) ->
    Counters = orddict:fetch_keys(DeltaMap),
    Min1 = case lists:member(Min0, Counters) of
               true ->
                   Min0;
               false ->
                   lists:min(Counters)
           end,
    SmallDeltaMap = orddict:filter(fun(Counter, _Delta) ->
                                           (Counter >= Min1) andalso (Counter < Max)
                                   end, DeltaMap),
    Deltas = orddict:fold(fun(_Counter, {Origin, Delta}, Deltas0) ->
                                  case Origin of
                                      Destination ->
                                        Deltas0;
                                      _ ->
                                        lasp_type:merge(Type, Deltas0, Delta)
                                  end
                          end, lasp_type:new_delta(Type), SmallDeltaMap),

    %% @todo should we remove this once we know everything is working fine?
    case lasp_type:is_delta(Type, Deltas) of
        false ->
            % lager:info("Folded delta group is not a delta");
            ok;
        true ->
            ok
    end,
    Deltas.

%% @private
declare_if_not_found({error, not_found}, {StorageId, TypeId},
                     #state{store=Store, actor=Actor}, Module, Function, Args) ->
    {ok, {{StorageId, TypeId}, _, _, _}} = ?CORE:declare(StorageId, TypeId,
                                                         ?CLOCK_INIT(Actor), Store),
    erlang:apply(Module, Function, Args);
declare_if_not_found(Result, _Id, _State, _Module, _Function, _Args) ->
    Result.

-ifdef(TEST).

do(Function, Args) ->
    Backend = lasp_ets_storage_backend,
    erlang:apply(Backend, Function, Args).

-else.

%% @doc Execute call to the proper backend.
do(Function, Args) ->
    Backend = lasp_config:get(storage_backend, lasp_ets_storage_backend),
    erlang:apply(Backend, Function, Args).

-endif.

%% @private
log_transmission({Type, Payload}, PeerCount) ->
    try
        case lasp_config:get(instrumentation, false) of
            true ->
                ok = lasp_instrumentation:transmission(Type, Payload, PeerCount),
                ok;
            false ->
                ok
        end
    catch
        _:Error ->
            lager:error("Couldn't log transmission: ~p", [Error]),
            ok
    end.

%% @private
schedule_aae_synchronization() ->
    ShouldAAESync = state_based_mode()
            andalso (not tutorial_mode())
            andalso (not broadcast_tree_mode())
            andalso (
              peer_to_peer_mode()
              orelse
              (
               client_server_mode()
               andalso
               not (i_am_server() andalso reactive_server())
              )
            ),

    lager:info("~n~n---------------------------~n ShouldAAESync ~p ~n~n---------------------------~n", [ShouldAAESync]),

    case ShouldAAESync of
        true ->
            Interval = lasp_config:get(aae_interval, 10000),
            case lasp_config:get(jitter, false) of
                true ->
                    %% Add random jitter.
                    Jitter = rand_compat:uniform(Interval),
                    timer:send_after(Interval + Jitter, aae_sync);
                false ->
                    timer:send_after(Interval, aae_sync)
            end;
        false ->
            ok
    end.

%% @private
schedule_delta_synchronization() ->
    ShouldDeltaSync = delta_based_mode()
            andalso (
              peer_to_peer_mode()
              orelse
              (
               client_server_mode()
               andalso
               not (i_am_server() andalso reactive_server())
              )
            ),

    case ShouldDeltaSync of
        true ->
            Interval = lasp_config:get(delta_interval, 10000),
            case lasp_config:get(jitter, false) of
                true ->
                    %% Add random jitter.
                    Jitter = rand_compat:uniform(Interval),
                    timer:send_after(Interval + Jitter, delta_sync);
                false ->
                    timer:send_after(Interval, delta_sync)
            end;
        false ->
            ok
    end.

%% @private
schedule_delta_garbage_collection() ->
    case delta_based_mode() of
        true ->
            timer:send_after(?DELTA_GC_INTERVAL, delta_gc);
        false ->
            ok
    end.
%% @private
schedule_plumtree_memory_report() ->
    case lasp_config:get(memory_report, false) of
        true ->
            timer:send_after(?PLUMTREE_MEMORY_INTERVAL, plumtree_memory_report);
        false ->
            ok
    end.

%% @private
schedule_memory_utilization_report() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            timer:send_after(?MEMORY_UTILIZATION_INTERVAL, memory_utilization_report);
        false ->
            ok
    end.

%% @private
plumtree_memory_report() ->
    PlumtreeBroadcast = erlang:whereis(plumtree_broadcast),
    lager:info("Plumtree message queue: ~p",
               [process_info(PlumtreeBroadcast, message_queue_len)]),
    lager:info("Our message queue: ~p",
               [process_info(self(), message_queue_len)]).

%% @private
memory_utilization_report() ->
    TotalBytes = orddict:fetch(total, erlang:memory()),
    lasp_instrumentation:memory(TotalBytes).

%% @private
send(Msg, Peer) ->
    log_transmission(extract_log_type_and_payload(Msg), 1),
    PeerServiceManager = lasp_config:peer_service_manager(),
    case PeerServiceManager:forward_message(Peer, ?MODULE, Msg) of
        ok ->
            ok;
        _Error ->
            % lager:error("Failed send to ~p for reason ~p", [Peer, Error]),
            ok
    end.

%% @private
extract_log_type_and_payload({aae_send, _Node, {_Id, _Type, _Metadata, State}}) ->
    {aae_send, State};
extract_log_type_and_payload({delta_send, _Node, {_Id, _Type, _Metadata, Deltas}, Counter}) ->
    {delta_send, {Deltas, Counter}};
extract_log_type_and_payload({delta_ack, _Node, _Id, Counter}) ->
    {delta_ack, Counter}.

%% @private
init_aae_sync(Peer, Store) ->
    ObjectFilterFun = fun(_) -> true end,
    init_aae_sync(Peer, ObjectFilterFun, Store).

%% @private
init_aae_sync(Peer, ObjectFilterFun, Store) ->
    lasp_logger:extended("Initializing AAE synchronization with peer: ~p", [Peer]),
    Function = fun({Id, #dv{type=Type, metadata=Metadata, value=Value}}, Acc0) ->
                    case orddict:find(dynamic, Metadata) of
                        {ok, true} ->
                            %% Ignore: this is a dynamic variable.
                            Acc0;
                        _ ->
                            case ObjectFilterFun(Id) of
                                true ->
                                    send({aae_send, node(), {Id, Type, Metadata, Value}}, Peer),
                                    [{ok, {Id, Type, Metadata, Value}}|Acc0];
                                false ->
                                    Acc0
                            end
                    end
               end,
    %% TODO: Should this be parallel?
    {ok, Result} = do(fold, [Store, Function, []]),
    lasp_logger:extended("Finished AAE synchronization with peer: ~p; sent ~p objects", [Peer, length(Result)]),
    ok.

%% @private
init_delta_sync(Peer, ObjectFilterFun) ->
    gen_server:cast(?MODULE, {delta_exchange, Peer, ObjectFilterFun}).

%% @private
membership() ->
    lasp_peer_service:members().

%% @private
select_random_sublist(List, K) ->
    lists:sublist(shuffle(List), K).

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{lasp_support:puniform(65535), N} || N <- L])].

%% @private
compute_exchange(Peers) ->
    PeerServiceManager = lasp_config:peer_service_manager(),

    Probability = lasp_config:get(partition_probability, 0),
    lager:info("Probability of partition: ~p", [Probability]),
    Percent = lasp_support:puniform(100),

    case Percent =< Probability of
        true ->
            case PeerServiceManager of
                partisan_client_server_peer_service_manager ->
                    lager:info("Partitioning from server."),
                    [];
                _ ->
                    lager:info("Partitioning ~p% of the network.",
                               [Percent]),

                    %% Select percentage, minus one node which will be
                    %% the server node.
                    K = round((Percent / 100) * length(Peers)),
                    lager:info("Partitioning ~p%: ~p nodes.",
                               [Percent, K]),
                    ServerNodes = case PeerServiceManager:active(server) of
                        {ok, undefined} ->
                            [];
                        {ok, Server} ->
                            [Server];
                        error ->
                            []
                    end,
                    lager:info("ServerNodes: ~p", [ServerNodes]),

                    Random = select_random_sublist(Peers, K),
                    RandomAndServer = lists:usort(ServerNodes ++ Random),
                    lager:info("Partitioning ~p from ~p during sync.",
                               [RandomAndServer, Peers -- RandomAndServer]),
                    Peers -- RandomAndServer
            end;
        false ->
            Peers
    end.

%% @private
buffer_broadcast(Payload) ->
    lasp_broadcast_buffer:buffer(Payload).

%% @private
without_me(Members) ->
    Members -- [node()].

%% @private
state_based_mode() ->
    lasp_config:get(mode, state_based) == state_based.

%% @private
delta_based_mode() ->
    lasp_config:get(mode, state_based) == delta_based.

%% @private
broadcast_tree_mode() ->
    lasp_config:get(broadcast, false).

%% @private
tutorial_mode() ->
    lasp_config:get(tutorial, false).

%% @private
client_server_mode() ->
    lasp_config:peer_service_manager() == partisan_client_server_peer_service_manager.

%% @private
peer_to_peer_mode() ->
    lasp_config:peer_service_manager() == partisan_hyparview_peer_service_manager.

%% @private
i_am_server() ->
    partisan_config:get(tag, undefined) == server.

%% @private
reactive_server() ->
    lasp_config:get(reactive_server, false).
