-define(TIMEOUT, 100000).

-define(APP, lasp).
-define(VNODE, lasp).

-define(BUCKET, <<"lasp">>).

-ifndef(EQC).

%% Erlang term storage based backend; no persistence.
% -define(BACKEND, lasp_ets_backend).

%% LevelDB backend; persistence.
-define(BACKEND, lasp_eleveldb_backend).

%% Bitcask backend.
% -define(BACKEND, lasp_bitcask_backend).

-else.

-define(BACKEND, lasp_ets_backend).

-endif.

%% Code which connects the backends to the actual backend
%% implementation.
-define(CORE, lasp_core).

%% Default set implementation for Lasp internal state tracking.
-define(SET, lasp_orset_gbtree).

%% What the process scope is in?
-define(SCOPE, lists).

-define(N, 3).
-define(W, 2).
-define(R, 2).

-define(PROGRAM_N, 3).
-define(PROGRAM_W, 2).
-define(PROGRAM_R, 2).

-define(PROCESS_R, 1).

-define(PROGRAM_KEY,    registered).
-define(PROGRAM_PREFIX, {lasp, programs}).

-record(read, {id :: id(),
               type :: type(),
               value :: value(),
               read_fun :: function()}).

-record(lasp_execute_request_v1, {
        module :: atom(),
        req_id :: non_neg_integer(),
        caller :: pid()}).

-define(EXECUTE_REQUEST, #lasp_execute_request_v1).

-record(dv, {value,
             waiting_threads = [],
             lazy_threads = [],
             type}).

-type variable() :: #dv{}.

-ifndef(namespaced_types).
-type module() :: atom().
-endif.

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
-type type() :: lasp_ivar | lasp_orset | lasp_orset_gbtree | lasp_gset | riak_dt_orswot | riak_dt_gcounter.
-type value() :: term().
-type func() :: atom().
-type args() :: list().
-type bound() :: true | false.
-type supervisor() :: pid().
-type stream() :: list(#dv{}).
-type store() :: ets:tid() | eleveldb:db_ref() | atom() | reference().
-type threshold() :: value() | {strict, value()}.
-type pending_threshold() :: {threshold, read | wait, pid(), type(), threshold()}.
-type operation() :: {atom(), value()} | {atom(), value(), value()}.
-type operations() :: list(operation()).
-type ivar() :: term().
-type actor() :: term().

%% @doc Result of a read operation.
-type var() :: {id(), type(), value()}.

%% @doc Only CRDTs are able to be processed.
-type crdt() :: riak_dt:crdt().

%% @doc Output of program must be a CRDT.
-type output() :: crdt().

%% @doc Any state that needs to be tracked from one execution to the
%%      next execution.
-type state() :: term().

%% @doc Reasons provided by the riak_kv vnode for change.
-type reason() :: put | handoff | delete.

%% @doc The type of objects that we can be notified about.
-type object() :: crdt().
