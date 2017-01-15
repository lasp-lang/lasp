%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(lasp_client_server_advertisement_counter_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

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
     client_server_state_based_test,
     client_server_delta_based_test,
     reactive_client_server_state_based_test,
     reactive_client_server_delta_based_test
     %%client_server_state_based_ps_test,
     %%client_server_delta_based_ps_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

default_test(_Config) ->
    ok.

%% ===================================================================
%% client/server with local replica
%% ===================================================================

client_server_state_based_test(Config) ->
    lasp_simulation_support:run(client_server_ad_counter_state_based_test,
        Config,
        [{mode, state_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, orset},
         {broadcast, false},
         {evaluation_identifier, client_server_state_based}]),
    ok.

client_server_delta_based_test(Config) ->
    lasp_simulation_support:run(client_server_ad_counter_delta_based_test,
        Config,
        [{mode, delta_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, orset},
         {broadcast, false},
         {evaluation_identifier, client_server_delta_based}]),
    ok.

reactive_client_server_state_based_test(Config) ->
    lasp_simulation_support:run(reactive_client_server_ad_counter_state_based_test,
        Config,
        [{mode, state_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, orset},
         {broadcast, false},
         {reactive_server, true},
         {evaluation_identifier, reactive_client_server_state_based}]),
    ok.

reactive_client_server_delta_based_test(Config) ->
    lasp_simulation_support:run(reactive_client_server_ad_counter_delta_based_test,
        Config,
        [{mode, delta_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, orset},
         {broadcast, false},
         {reactive_server, true},
         {evaluation_identifier, reactive_client_server_delta_based}]),
    ok.

client_server_state_based_ps_test(Config) ->
    lasp_simulation_support:run(client_server_ad_counter_state_based_ps_test,
        Config,
        [{mode, state_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, awset_ps},
         {broadcast, false},
         {evaluation_identifier, client_server_state_based_ps}]),
    ok.

client_server_delta_based_ps_test(Config) ->
    lasp_simulation_support:run(client_server_ad_counter_delta_based_ps_test,
        Config,
        [{mode, delta_based},
         {simulation, ad_counter},
         {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
         {set, awset_ps},
         {broadcast, false},
         {evaluation_identifier, client_server_delta_based_ps}]),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================
