-module(derflow_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = {derflow_vnode_master,
               {riak_core_vnode_master, start_link, [derflow_vnode]},
                permanent, 5000, worker, [riak_core_vnode_master]},

    DeclareFSM = {derflow_declare_fsm_sup,
                  {derflow_declare_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [derflow_declare_fsm_sup]},

    RegisterFSM = {derflow_register_fsm_sup,
                   {derflow_register_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [derflow_register_fsm_sup]},

    ExecuteFSM = {derflow_execute_fsm_sup,
                  {derflow_execute_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [derflow_execute_fsm_sup]},

    {ok, {{one_for_one, 5, 10}, [VMaster,
                                 DeclareFSM,
                                 RegisterFSM,
                                 ExecuteFSM]}}.
