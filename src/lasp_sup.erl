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

-module(lasp_sup).

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
    VMaster = {lasp_vnode_master,
               {riak_core_vnode_master, start_link, [lasp_vnode]},
                permanent, 5000, worker, [riak_core_vnode_master]},

    DeclareFSM = {lasp_declare_fsm_sup,
                  {lasp_declare_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [lasp_declare_fsm_sup]},

    UpdateFSM = {lasp_update_fsm_sup,
                 {lasp_update_fsm_sup, start_link, []},
                  permanent, infinity, supervisor, [lasp_update_fsm_sup]},

    BindFSM = {lasp_bind_fsm_sup,
               {lasp_bind_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [lasp_bind_fsm_sup]},

    BindToFSM = {lasp_bind_to_fsm_sup,
               {lasp_bind_to_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [lasp_bind_to_fsm_sup]},

    RegisterFSM = {lasp_register_fsm_sup,
                   {lasp_register_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [lasp_register_fsm_sup]},

    RegisterGlobalFSM = {lasp_register_global_fsm_sup,
                         {lasp_register_global_fsm_sup, start_link, []},
                          permanent, infinity, supervisor, [lasp_register_global_fsm_sup]},

    ExecuteFSM = {lasp_execute_fsm_sup,
                  {lasp_execute_fsm_sup, start_link, []},
                   permanent, infinity, supervisor, [lasp_execute_fsm_sup]},

    ExecuteCoverageFSM = {lasp_execute_coverage_fsm_sup,
                          {lasp_execute_coverage_fsm_sup, start_link, []},
                           permanent, infinity, supervisor, [lasp_execute_coverage_fsm_sup]},

    {ok, {{one_for_one, 5, 10}, [VMaster,
                                 DeclareFSM,
                                 UpdateFSM,
                                 BindFSM,
                                 BindToFSM,
                                 RegisterFSM,
                                 RegisterGlobalFSM,
                                 ExecuteFSM,
                                 ExecuteCoverageFSM]}}.
