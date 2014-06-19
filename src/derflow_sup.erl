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

    BindFSM = {derflow_bind_fsm_sup,
                  {derflow_bind_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_bind_fsm_sup]},

    ReadFSM = {derflow_read_fsm_sup,
                  {derflow_read_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_read_fsm_sup]},

    TouchFSM = {derflow_touch_fsm_sup,
                  {derflow_touch_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_touch_fsm_sup]},

    NextFSM = {derflow_next_fsm_sup,
                  {derflow_next_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_next_fsm_sup]},

    IsDetFSM = {derflow_is_det_fsm_sup,
                  {derflow_is_det_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_is_det_fsm_sup]},

    DeclareFSM = {derflow_declare_fsm_sup,
                  {derflow_declare_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_declare_fsm_sup]},

    WaitNeededFSM = {derflow_wait_needed_fsm_sup,
                  {derflow_wait_needed_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [derflow_wait_needed_fsm_sup]},

    {ok,
        {{one_for_one, 5, 10},
         [VMaster, BindFSM, ReadFSM, TouchFSM, NextFSM, IsDetFSM,
          DeclareFSM, WaitNeededFSM]}}.
