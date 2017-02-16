%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(lasp_client_server_throughput_SUITE).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include("lasp.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(RUNS, [1,2,4]).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

all() ->
    [
     client_server_state_based_gcounter,
     client_server_state_based_boolean,
     client_server_state_based_gset
    ].

%% ===================================================================
%% tests
%% ===================================================================

default_test(_Config) ->
    ok.

%% ===================================================================
%% peer-to-peer
%% ===================================================================

client_server_state_based_gcounter(Config) ->
    lists:foreach(fun(N) ->
                        lasp_simulation_support:run(client_server_state_based_gcounter,
                            Config,
                            [{mode, state_based},
                             {client_number, N},
                             {simulation, throughput},
                             {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
                             {set, orset},
                             {throughput_type, gcounter},
                             {broadcast, false},
                             {blocking_sync, true},
                             {evaluation_identifier, client_server_state_based_gcounter}])
                  end, ?RUNS),
    ok.

client_server_state_based_gset(Config) ->
    lists:foreach(fun(N) ->
                        lasp_simulation_support:run(client_server_state_based_gset,
                            Config,
                            [{mode, state_based},
                             {client_number, N},
                             {simulation, throughput},
                             {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
                             {set, orset},
                             {throughput_type, gset},
                             {broadcast, false},
                             {blocking_sync, true},
                             {evaluation_identifier, client_server_state_based_gset}])
                  end, ?RUNS),
    ok.

client_server_state_based_boolean(Config) ->
    lists:foreach(fun(N) ->
                        lasp_simulation_support:run(client_server_state_based_boolean,
                            Config,
                            [{mode, state_based},
                             {client_number, N},
                             {simulation, throughput},
                             {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
                             {set, orset},
                             {throughput_type, boolean},
                             {broadcast, false},
                             {blocking_sync, true},
                             {evaluation_identifier, client_server_state_based_boolean}])
                  end, ?RUNS),
    ok.
