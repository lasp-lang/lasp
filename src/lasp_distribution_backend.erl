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

-module(lasp_distribution_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% Administrative controls.
-export([reset/0, 
         propagate/1]).

%% API
-export([start_link/0,
         start_link/1]).

%% lasp interface
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
         thread/3,
         enforce_once/3]).

-export([interested/1,
         disinterested/1,
         set_topic/2,
         remove_topic/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% debug callbacks
-export([local_bind/4]).

%% blocking sync helper.
-export([blocking_sync/1]).

-include("lasp.hrl").

%% State record.
-record(state, {store :: store(), gossip_peers :: [], actor :: binary()}).

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
%%% lasp_distribution_backend callbacks
%%%===================================================================

%% @doc Declare a new dataflow variable of a given type.
-spec declare(id(), type()) -> {ok, var()}.
declare(Id, Type) ->
    gen_server:call(?MODULE, {declare, Id, Type}, infinity).

%% @doc Declare a new dynamic variable of a given type.
-spec declare_dynamic(id(), type()) -> {ok, var()}.
declare_dynamic(Id, Type) ->
    gen_server:call(?MODULE, {declare_dynamic, Id, Type}, infinity).

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
    gen_server:call(?MODULE, {update, Id, Operation, Actor}, infinity).

%% @doc Bind a dataflow variable to a value.
%%
%%      The provided `Id' is identifier of a previously declared (see:
%%      {@link declare/0}) dataflow variable.  The `Value' provided is
%%      the value to bind.
%%
-spec bind(id(), value()) -> {ok, var()}.
bind(Id, Value0) ->
    gen_server:call(?MODULE, {bind, Id, Value0}, infinity).

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

%% @doc Enforce a invariant once over a monotonic condition.
%%
-spec enforce_once(id(), threshold(), function()) -> ok.
enforce_once(Id, Threshold, EnforceFun) ->
    gen_server:call(?MODULE, {enforce_once, Id, Threshold, EnforceFun}, infinity).

%% @doc Pause execution until value requested with given threshold.
%%
%%      Pause execution of calling thread until a read operation is
%%      issued for the given `Id'.  Used to introduce laziness into a
%%      computation.
%%
-spec wait_needed(id(), threshold()) -> ok | error().
wait_needed(Id, Threshold) ->
    gen_server:call(?MODULE, {wait_needed, Id, Threshold}, infinity).

%% @todo
interested(Topic) ->
    gen_server:call(?MODULE, {interested, Topic}, infinity).

%% @todo
disinterested(Topic) ->
    gen_server:call(?MODULE, {disinterested, Topic}, infinity).

%% @todo
set_topic(Id, Topic) ->
    gen_server:call(?MODULE, {set_topic, Id, Topic}, infinity).

%% @todo
remove_topic(Id, Topic) ->
    gen_server:call(?MODULE, {remove_topic, Id, Topic}, infinity).

%%%===================================================================
%%% Administrative controls
%%%===================================================================

%% @doc Reset all Lasp application state.
-spec reset() -> ok.
reset() ->
    gen_server:call(?MODULE, reset, infinity).

-spec propagate(id()) -> ok.
propagate(Id) ->
    gen_server:call(?MODULE, {propagate, Id}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Seed the process at initialization.
    ?SYNC_BACKEND:seed(),

    {ok, Actor} = lasp_unique:unique(),
    lasp_config:set(actor, Actor),

    Identifier = node(),

    %% Start the storage backend.
    {ok, Store} = case ?CORE:start_link(Identifier) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            _ = lager:error("Failed to initialize backend: ~p", [Reason]),
            {error, Reason}
    end,

    %% Start synchronization backend.
    case lasp_config:get(mode, ?DEFAULT_MODE) of
        state_based ->
            lasp_state_based_synchronization_backend:start_link([Store, Actor]),
            ok;
        delta_based ->
            lasp_delta_based_synchronization_backend:start_link([Store, Actor]),
            ok
    end,

    {ok, #state{actor=Actor, gossip_peers=[], store=Store}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({propagate, Id}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("propagate"),

    ok = do_propagate(Id, Store),

    {reply, ok, State};

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
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("declare/2"),

    Result = ?CORE:declare(Id, Type, ?CLOCK_INIT(Actor), Store),
    {reply, Result, State};

%% @todo
handle_call({interested, Topic}, _From, 
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("interested/1"),

    Myself = partisan_peer_service_manager:myself(),
    Id = ?INTERESTS_ID,
    Operation = {apply, Myself, {add, Topic}},
    Result0 = ?CORE:update(Id, Operation, Actor, ?CLOCK_INCR(Actor),
                            ?CLOCK_INIT(Actor), Store),
    Final = {ok, {_, _, Metadata, _}} = declare_if_not_found(Result0, Id, State, ?CORE, update,
                            [Id, Operation, Actor, ?CLOCK_INCR(Actor), Store]),

    case lasp_config:get(propagate_on_update, false) of
        true ->
            ok = do_propagate(Id, Metadata, Store);
        false ->
            ok
    end,
    case lasp_config:get(blocking_sync, false) of
        true ->
            ok = blocking_sync(Id, Metadata);
        false ->
            ok
    end,

    {reply, Final, State};

%% @todo
handle_call({disinterested, Topic}, _From, 
             #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("disinterested/1"),

    Myself = partisan_peer_service_manager:myself(),
    Id = ?INTERESTS_ID,
    Operation = {apply, Myself, {rmv, Topic}},
    Result0 = ?CORE:update(Id, Operation, Actor, ?CLOCK_INCR(Actor),
                            ?CLOCK_INIT(Actor), Store),
    Final = {ok, {_, _, Metadata, _}} = declare_if_not_found(Result0, Id, State, ?CORE, update,
                            [Id, Operation, Actor, ?CLOCK_INCR(Actor), Store]),

    case lasp_config:get(propagate_on_update, false) of
        true ->
            ok = do_propagate(Id, Metadata, Store);
        false ->
            ok
    end,
    case lasp_config:get(blocking_sync, false) of
        true ->
            ok = blocking_sync(Id, Metadata);
        false ->
            ok
    end,

    {reply, Final, State};

%% @todo
handle_call({set_topic, Id, Topic}, _From, 
             #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("set_topic/2"),

    Type = lasp_type:get_type(?OBJECT_INTERESTS_TYPE),

    MetadataFunDeclare = fun(Metadata) ->
        NewTopicSet = Type:mutate({add, Topic}, Actor, Type:new()),
        orddict:store(topics, NewTopicSet, Metadata)
    end,

    MetadataFun = fun(Metadata) ->
        case orddict:find(topics, Metadata) of
            error ->
                MetadataFunDeclare(Metadata);
            {ok, V} ->
                TopicSet = Type:mutate({add, Topic}, Actor, V),
                orddict:store(topics, TopicSet, Metadata)
        end
    end,

    {ok, _} = ?CORE:update_metadata(Id, Actor, MetadataFun, MetadataFunDeclare, Store),

    {reply, ok, State};

%% @todo
handle_call({remove_topic, Id, Topic}, _From, 
             #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("remove_topic/2"),

    Type = lasp_type:get_type(?OBJECT_INTERESTS_TYPE),

    MetadataFunDeclare = fun(Metadata) ->
        orddict:store(topics, Type:new(), Metadata)
    end,

    MetadataFun = fun(Metadata) ->
        case orddict:find(topics, Metadata) of
            error ->
                MetadataFunDeclare(Metadata);
            {ok, V} ->
                TopicSet = Type:mutate({rmv, Topic}, Actor, V),
                orddict:store(topics, TopicSet, Metadata)
        end
    end,

    {ok, _} = ?CORE:update_metadata(Id, Actor, MetadataFun, MetadataFunDeclare, Store),

    {reply, ok, State};

%% Incoming bind request, where we do not have information about the
%% variable yet.  In this case, take the remote metadata, if we don't
%% have local metadata.
handle_call({declare, Id, IncomingMetadata, Type}, _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("declare/3"),

    Metadata0 = case get(Id, Store) of
        {ok, {_, _, Metadata, _}} ->
            Metadata;
        {error, _Error} ->
            IncomingMetadata
    end,
    Result = ?CORE:declare(Id, Type, ?CLOCK_MERG, Metadata0, Store),
    {reply, Result, State};

%% Issue a local dynamic declare operation, which should initialize
%% metadata and result in the broadcast operation.
handle_call({declare_dynamic, Id, Type}, _From,
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("declare_dynamic"),

    Result = ?CORE:declare_dynamic(Id, Type, ?CLOCK_INIT(Actor), Store),
    {reply, Result, State};

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
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind/2"),

    Result = case lasp_config:get(intermediary_node_modification,
                                  ?INTERMEDIARY_NODE_MODIFICATION) of
        true ->
            Result0 = ?CORE:bind(Id, Value, ?CLOCK_INCR(Actor), Store),
            declare_if_not_found(Result0, Id, State, ?CORE, bind,
                                 [Id, Value, ?CLOCK_INCR(Actor), Store]);
        false ->
            {ok, IsRoot} = case ?DAG_ENABLED of
                               true ->
                                   lasp_dependence_dag:is_root(Id);
                               false ->
                                   {ok, true}
                           end,
            case IsRoot of
                false ->
                    {error, {intermediary_not_modification_prohibited, Id}};
                true ->
                    Result0 = ?CORE:bind(Id, Value, ?CLOCK_INCR(Actor), Store),
                    declare_if_not_found(Result0, Id, State, ?CORE, bind,
                                         [Id, Value, ?CLOCK_INCR(Actor), Store])
            end
    end,

    {reply, Result, State};

%% Incoming bind operation; merge incoming clock with the remote clock.
%%
%% Note: we don't need to rebroadcast, because if the value resulting
%% from the merge changes and we issue re-bind, it should trigger the
%% clock to be incremented again and a subsequent broadcast.  However,
%% this could change if the other module is later refactored.
handle_call({bind, Id, Metadata0, Value}, _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind/3"),

    Result0 = ?CORE:bind(Id, Value, ?CLOCK_MERG, Store),
    Result = declare_if_not_found(Result0, Id, State, ?CORE, bind,
                                  [Id, Value, ?CLOCK_MERG, Store]),
    {reply, Result, State};

%% Bind two variables together.
handle_call({bind_to, Id, DVId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("bind_to"),

    {ok, _Pid} = ?CORE:bind_to(Id, DVId, Store, ?CORE_WRITE, ?CORE_READ),
    {reply, ok, State};

%% Perform an update, and ensure that we bump the logical clock as we
%% perform the update.
%%
%% The CRDT actor is used to distinguish actors *per-thread*, if
%% necessary, where the vclock is serialized *per-node*.
%%
handle_call({update, Id, Operation, CRDTActor}, _From,
            #state{store=Store, actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("update"),

    Result = case lasp_config:get(intermediary_node_modification,
                                  ?INTERMEDIARY_NODE_MODIFICATION) of
        true ->
            Result0 = ?CORE:update(Id, Operation, CRDTActor, ?CLOCK_INCR(Actor),
                                   ?CLOCK_INIT(Actor), Store),
            Final = {ok, {_, _, Metadata, _}} = declare_if_not_found(Result0, Id, State, ?CORE, update,
                                 [Id, Operation, Actor, ?CLOCK_INCR(Actor), Store]),
            case lasp_config:get(propagate_on_update, false) of
                true ->
                    ok = do_propagate(Id, Metadata, Store);
                false ->
                    ok
            end,
            case lasp_config:get(blocking_sync, false) of
                true ->
                    ok = blocking_sync(Id, Metadata);
                false ->
                    ok
            end,
            Final;
        false ->
            {ok, IsRoot} = case ?DAG_ENABLED of
                               true ->
                                   lasp_dependence_dag:is_root(Id);
                               false ->
                                   {ok, true}
                           end,
            case IsRoot of
                false ->
                    {error, {intermediary_not_modification_prohibited, Id}};
                true ->
                    Result0 = ?CORE:update(Id, Operation, CRDTActor, ?CLOCK_INCR(Actor),
                                           ?CLOCK_INIT(Actor), Store),
                    Final = {ok, {_, _, Metadata, _}} = declare_if_not_found(Result0, Id, State, ?CORE, update,
                                         [Id, Operation, Actor, ?CLOCK_INCR(Actor), Store]),
                    case lasp_config:get(propagate_on_update, false) of
                        true ->
                            ok = do_propagate(Id, Metadata, Store);
                        false ->
                            ok
                    end,
                    case lasp_config:get(blocking_sync, false) of
                        true ->
                            ok = blocking_sync(Id, Metadata);
                        false ->
                            ok
                    end,
                    Final
            end
    end,

    {reply, Result, State};

handle_call({enforce_once, Id, Threshold, EnforceFun},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("enforce_once"),

    {ok, _Pid} = ?CORE:enforce_once(Id, Threshold, EnforceFun, Store),

    {reply, ok, State};

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
    ?CORE:wait_needed(Id, Threshold, Store, From, ReplyFun, ?NOREPLY);

%% Attempt to read a value.
handle_call({read, Id, Threshold}, From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("read"),

    ReplyFun = fun({_Id, Type, Metadata, Value}) ->
                    {reply, {ok, {_Id, Type, Metadata, Value}}, State};
                  ({error, Error}) ->
                    {reply, {error, Error}, State}
               end,
    Value0 = ?CORE:read(Id, Threshold, Store, From, ReplyFun, ?NOREPLY),
    declare_if_not_found(Value0, Id, State, ?CORE, read,
                         [Id, Threshold, Store, From, ReplyFun, ?NOREPLY]);

%% Spawn a process to perform a filter.
handle_call({filter, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("filter"),

    {ok, _Pid} = ?CORE:filter(Id, Function, AccId, Store, ?CORE_WRITE, ?CORE_READ),
    {reply, ok, State};

%% Spawn a process to compute a product.
handle_call({product, Left, Right, Product},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("product"),

    {ok, _Pid} = ?CORE:product(Left, Right, Product, Store, ?CORE_WRITE,
                               ?CORE_READ, ?CORE_READ),
    {reply, ok, State};

%% Spawn a process to compute the intersection.
handle_call({intersection, Left, Right, Intersection},
            _From,
            #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("intersection"),

    {ok, _Pid} = ?CORE:intersection(Left, Right, Intersection, Store,
                                    ?CORE_WRITE, ?CORE_READ, ?CORE_READ),
    {reply, ok, State};

%% Spawn a process to compute the union.
handle_call({union, Left, Right, Union}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("union"),

    {ok, _Pid} = ?CORE:union(Left, Right, Union, Store, ?CORE_WRITE, ?CORE_READ,
                             ?CORE_READ),
    {reply, ok, State};

%% Spawn a process to perform a map.
handle_call({map, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("map"),

    {ok, _Pid} = ?CORE:map(Id, Function, AccId, Store, ?CORE_WRITE, ?CORE_READ),
    {reply, ok, State};

%% Spawn a process to perform a fold.
handle_call({fold, Id, Function, AccId}, _From, #state{store=Store}=State) ->
    lasp_marathon_simulations:log_message_queue_size("fold"),

    {ok, _Pid} = ?CORE:fold(Id, Function, AccId, Store, ?CORE_BIND, ?CORE_READ),
    {reply, ok, State};

handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled call messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

handle_info(Msg, State) ->
    _ = lager:warning("Unhandled info messages: ~p", [Msg]),
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
declare_if_not_found({error, not_found}, {StorageId, TypeId},
                     #state{store=Store, actor=Actor}, Module, Function, Args) ->
    {ok, {{StorageId, TypeId}, _, _, _}} = ?CORE:declare(StorageId, TypeId,
                                                         ?CLOCK_INIT(Actor), Store),
    erlang:apply(Module, Function, Args);
declare_if_not_found(Result, _Id, _State, _Module, _Function, _Args) ->
    Result.

%% @private
do_propagate(Id, Store) ->
    ReplyFun = fun({_Id, Type, Metadata, Value}) ->
                    {ok, {_Id, Type, Metadata, Value}};
                  ({error, Error}) ->
                    {error, Error}
               end,
    BlockingFun = fun() ->
            {error, blocking}
    end,
    Threshold = undefined,
    case ?CORE:read(Id, Threshold, Store, self(), ReplyFun, BlockingFun) of
        {ok, {Id, _Type, Metadata, _Value}} ->
            do_propagate(Id, Metadata, Store);
        _ ->
            ok
    end.

%% @private
do_propagate(Id, Metadata, _Store) ->
    case orddict:find(dynamic, Metadata) of
        {ok, true} ->
            %% Ignore: this is a dynamic variable.
            ok;
        _ ->
            ObjectFilterFun = fun(I, _) -> Id == I end,

            case lasp_config:get(mode, ?DEFAULT_MODE) of
                state_based ->
                    lasp_state_based_synchronization_backend:propagate(ObjectFilterFun);
                delta_based ->
                    {error, not_implemented}
            end
    end.

%% @private
blocking_sync(Id, Metadata) ->
    case orddict:find(dynamic, Metadata) of
        {ok, true} ->
            %% Ignore: this is a dynamic variable.
            ok;
        _ ->
            ObjectFilterFun = fun(I, _) -> Id == I end,

            case lasp_config:get(mode, ?DEFAULT_MODE) of
                state_based ->
                    lasp_state_based_synchronization_backend:blocking_sync(ObjectFilterFun);
                delta_based ->
                    {error, not_implemented}
            end
    end.

%% @private
blocking_sync(ObjectFilterFun) ->
    case lasp_config:get(mode, ?DEFAULT_MODE) of
        state_based ->
            lasp_state_based_synchronization_backend:blocking_sync(ObjectFilterFun);
        delta_based ->
            {error, not_implemented}
    end.
