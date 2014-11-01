%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

    RegisterGlobalFSM = {derflow_register_global_fsm_sup,
                         {derflow_register_global_fsm_sup, start_link, []},
                          permanent, infinity, supervisor, [derflow_register_global_fsm_sup]},

    ExecuteFSM = {derflow_execute_fsm_sup,
                  {derflow_execute_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [derflow_execute_fsm_sup]},

    ExecuteCoverageFSM = {derflow_execute_coverage_fsm_sup,
                          {derflow_execute_coverage_fsm_sup, start_link, []},
                           permanent, infinity, supervisor, [derflow_execute_coverage_fsm_sup]},

    {ok, {{one_for_one, 5, 10}, [VMaster,
                                 DeclareFSM,
                                 RegisterFSM,
                                 RegisterGlobalFSM,
                                 ExecuteFSM,
                                 ExecuteCoverageFSM]}}.
