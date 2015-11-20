-define(TIMEOUT, 100000).

-define(APP, lasp).

%% Code which connects the storage backends to the implementation.
-define(CORE, lasp_core).

%% Default set implementation for Lasp internal state tracking.
-define(SET, lasp_orset_gbtree).

%% What the process scope is in?
-define(SCOPE, lists).

-define(PROGRAM_KEY,    registered).
-define(PROGRAM_PREFIX, {lasp, programs}).

-record(read, {id :: id(),
               type :: type(),
               value :: value(),
               read_fun :: function()}).

-record(dv, {value :: value(),
             waiting_threads = [] :: list(pending_threshold()),
             lazy_threads = [] :: list(pending_threshold()),
             type :: type(),
             metadata :: metadata()}).

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

%% General types.
-type file() :: iolist().
-type registration() :: preflist | global.
-type id() :: binary().
-type idx() :: term().
-type result() :: term().
-type type() :: lasp_ivar | lasp_orset | lasp_orset_gbtree | lasp_top_k_var | {lasp_top_k_var, [any()]} | riak_dt_gcounter.
-type value() :: term().
-type func() :: atom().
-type args() :: list().
-type bound() :: true | false.
-type supervisor() :: pid().
-type stream() :: list(#dv{}).
-type store() :: ets:tid() | eleveldb:db_ref() | atom() | reference() | pid().
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
