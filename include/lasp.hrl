%% General timeout value.
-define(TIMEOUT, 100000).

%% Application information.
-define(APP, lasp).

%% Peer service port.
-define(PEER_PORT, 9000).

%% Number of additional nodes to run for the test suite.
-define(NUM_NODES, 3).

%% Code which connects the storage backends to the implementation.
-define(CORE, lasp_core).

%% Default set implementation for Lasp internal state tracking.
-define(SET, lasp_orset).

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

%% @doc Garbage collection will happen after the certain number
%%      (MAX_GC_COUNTER) of times of exchanges.
-define(MAX_GC_COUNTER, 7).

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
-type operation() :: {atom(), value()} | {atom(), value(), value()}.
-type operations() :: list(operation()).
-type actor() :: term().
-type error() :: {error, atom()}.
-type metadata() :: orddict:orddict().

%% @doc Result of a read operation.
-type var() :: {id(), type(), metadata(), value()}.

%% @doc Only CRDTs are able to be processed.
-type crdt() :: riak_dt:crdt().

%% @doc Output of program must be a CRDT.
-type output() :: crdt().

%% @doc Any state that needs to be tracked from one execution to the
%%      next execution.
-type state() :: term().

%% @doc The type of objects that we can be notified about.
-type object() :: crdt().
