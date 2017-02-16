%% Throughput simulation.
-define(SIMPLE_BOOLEAN, {<<"boolean">>, ?BOOLEAN_TYPE}).
-define(SIMPLE_BAG, {<<"bag">>, ?GSET_TYPE}).
-define(SIMPLE_COUNTER, {<<"counter">>, ?GCOUNTER_TYPE}).
-define(EVENT_INTERVAL, 0). %% seconds
-define(MAX_EVENTS_DEFAULT, 5000).
-define(BATCH_EVENTS, 100).
-define(BOOLEAN_TYPE, boolean).

%% Intermediary node modification.
-define(INTERMEDIARY_NODE_MODIFICATION, false).

%% Reverse topological propagation.
-define(REVERSE_TOPOLOGICAL_SYNC, false).

%% DAG tracking enabled.
-define(DAG_ENABLED, false).

%% Synchronization backend.
-define(SYNC_BACKEND, lasp_synchronization_backend).

%% Delta synchronization interval.
-define(DELTA_GC_INTERVAL, 60000).

%% Plumtree peer refresh interval.
-define(PLUMTREE_PEER_REFRESH_INTERVAL, 10000).

%% Default mode.
-define(DEFAULT_MODE, state_based).

%% Synchronize state interval.
-define(STATE_SYNC_INTERVAL, 10000).

%% Report interval.
-define(MEMORY_UTILIZATION_INTERVAL, 10000).

%% Report interval.
-define(PLUMTREE_MEMORY_INTERVAL, 10000).

%% General timeout value.
-define(TIMEOUT, 100000).

%% Application information.
-define(APP, lasp).

%% Peer service port.
-define(PEER_PORT, 9000).

%% Code which connects the storage backends to the implementation.
-define(CORE, lasp_core).

%% Default set implementation for Lasp internal state tracking.
-define(SET, orset).

-record(read, {id :: id(),
               type :: type(),
               value :: value(),
               read_fun :: function()}).

-record(dv, {value :: value(),
             waiting_delta_threads = [] :: list(pending_threshold()),
             waiting_threads = [] :: list(pending_threshold()),
             lazy_threads = [] :: list(pending_threshold()),
             type :: type(),
             metadata :: metadata(),
             delta_counter :: non_neg_integer(),
             delta_map :: orddict:orddict(),
             delta_ack_map :: orddict:orddict()}).

-type variable() :: #dv{}.

%% @doc Generate a request id.
-define(REQID(), erlang:phash2(erlang:now())).

%% @doc Wait for a response.
%%
%%      Helper function; given a `ReqId', wait for a message within
%%      `Timeout' seconds and return the result.
%%
-define(WAIT(ReqId, Timeout),
        receive
            {ReqId, ok} ->
                ok;
            {ReqId, ok, Val} ->
                {ok, Val}
        after Timeout ->
            {error, timeout}
        end).

%% @doc A node will be evicted from the Ack Map when the entry of
%%      that node in the Ack Map remains unchanged for at least
%%      (MAX_GC_COUNTER) exchanges.
%%      If the delta exchange timer is 5 seconds and MAX_GC_COUNTER
%%      is 20, a node will be evicted from the Ack Map after not
%%      sending acks for +- 2 minutes. The actual time for eviction
%%      depends on when the gargabe collection happens.
-define(MAX_GC_COUNTER, 20).

%% General types.
-type file() :: iolist().
-type registration() :: preflist | global.
-type id() :: binary() | {binary(), type()}.
-type idx() :: term().
-type not_found() :: {error, not_found}.
-type result() :: term().
-type type() :: term().
-type value() :: term().
-type func() :: atom().
-type args() :: list().
-type bound() :: true | false.
-type supervisor() :: pid().
-type stream() :: list(#dv{}).
-type store() :: ets:tid() | atom() | reference() | pid().
-type threshold() :: value() | {strict, value()}.
-type pending_threshold() :: {threshold, read | wait, pid(), type(), threshold()}.
-type operation() :: {atom(), value()} | {atom(), value(), value()} | atom().
-type operations() :: list(operation()).
-type actor() :: term().
-type error() :: {error, atom()}.
-type metadata() :: orddict:orddict().

%% @doc Result of a read operation.
-type var() :: {id(), type(), metadata(), value()}.

%% @doc Only CRDTs are able to be processed.
-type crdt() :: type:crdt().

%% @doc Output of program must be a CRDT.
-type output() :: crdt().

%% @doc Any state that needs to be tracked from one execution to the
%%      next execution.
-type state() :: term().

%% @doc The type of objects that we can be notified about.
-type object() :: crdt().

%% Test identifiers.
-define(ADS, {<<"ads">>, ?SET_TYPE}).
-define(CONTRACTS, {<<"contracts">>, ?SET_TYPE}).
-define(ADS_CONTRACTS, {<<"ads_contracts">>, ?SET_TYPE}).
-define(ADS_WITH_CONTRACTS, {<<"ads_with_contracts">>, ?SET_TYPE}).

-define(MAX_PLAYERS_DEFAULT, 5).
-define(GAMES_NUMBER, 1).
-define(ENROLLABLE_GAMES, {<<"enrollable_games">>, ?SET_TYPE}).
-define(ENROLLMENT_INTERVAL, 500).

-define(COUNTER_TYPE, gcounter).
-define(GMAP_TYPE, gmap).
-define(GSET_TYPE, gset).
-define(GCOUNTER_TYPE, gcounter).
-define(SET_TYPE, lasp_config:get(set, orset)).
-define(PAIR_TYPE, pair).

%% Simulation status tracking code.
-define(SIM_STATUS_TRACKING, <<"status_tracking">>).

-define(SIM_STATUS_STRUCTURE,
        {?GMAP_TYPE, [
            {?PAIR_TYPE, [
                ?BOOLEAN_TYPE, %% observed ads disabled
                ?BOOLEAN_TYPE  %% logs pushed
            ]}
        ]}).

-define(SIM_STATUS_ID,
        {?SIM_STATUS_TRACKING, ?SIM_STATUS_STRUCTURE}).

%% Simulation helpers.
-define(STATE_INTERVAL, 10000).
-define(IMPRESSION_INTERVAL, 10000). %% 10 seconds
-define(STATUS_INTERVAL, 10000).
-define(EVAL_NUMBER, 1).
-define(LOG_INTERVAL, 10000).
-define(ADS_NUMBER, 1).

%% Storage backend.
-define(DEFAULT_STORAGE_BACKEND, lasp_ets_storage_backend).

%% Lexer and parser.
-define(SQL_LEXER, lasp_sql_lexer).
-define(SQL_PARSER, lasp_sql_parser).

-record(ad, {id, name, image, counter, register}).
-record(contract, {id}).

-define(MAX_IMPRESSIONS_DEFAULT, 10).

%% Definitions for the bind/read fun abstraction.

-define(CORE_BIND, fun(_AccId, _AccValue, _Store) ->
                ?CORE:bind(_AccId, _AccValue, _Store)
              end).

-define(CORE_WRITE, fun(_Store) ->
                 fun(_AccId, _AccValue) ->
                   {ok, _} = ?CORE:bind_var(_AccId, _AccValue, _Store)
                 end
               end).

-define(CORE_READ, fun(_Id, _Threshold) ->
                ?CORE:read_var(_Id, _Threshold, Store)
              end).

-define(NOREPLY, fun() -> {noreply, State} end).

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
