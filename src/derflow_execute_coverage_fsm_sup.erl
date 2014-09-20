-module(derflow_execute_coverage_fsm_sup).
-author('Christopher Meiklejohn <cmeiklejohn@basho.com>').

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1,
         terminate_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a child.
start_child(Args) ->
    lager:info("Starting coverage FSM from supervisor!"),
    supervisor:start_child(?MODULE, Args).

%% @doc Stop a child immediately.
terminate_child(Supervisor, Pid) ->
    supervisor:terminate_child(Supervisor, Pid).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc Supervisor callback.
init([]) ->
    Spec = {undefined,
            {riak_core_coverage_fsm, start_link, [derflow_execute_coverage_fsm]},
             temporary, 5000, worker, [derflow_execute_coverage_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [Spec]}}.
