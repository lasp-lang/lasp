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

-module(lasp_advertisement_counter_partition_overcounting_SUITE).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([suite/0,
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

suite() ->
    [{timetrap, infinity}].

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
     client_server_partition_overcounting_test,
     peer_to_peer_partition_overcounting_test,
     code_peer_to_peer_partition_overcounting_test
    ].

-define(CLIENTS, 4).

%% ===================================================================
%% tests
%% ===================================================================

default_test(_Config) ->
    ok.

client_server_partition_overcounting_test(Config) ->
    lists:foreach(
        fun(PartitionProbability) ->
            lasp_config:set(client_number, ?CLIENTS),
            lasp_config:set(partition_probability, PartitionProbability),
            EvaluationIdentifier = list_to_atom("client_server_partition_overcounting_" ++ integer_to_list(PartitionProbability)),

            lasp_simulation_support:run(client_server_partition_overcounting_test,
                Config,
                [{mode, delta_based},
                 {simulation, ad_counter_partition_overcounting},
                 {partisan_peer_service_manager, partisan_client_server_peer_service_manager},
                 {set, orset},
                 {broadcast, false},
                 {instrumentation, false},
                 {heavy_client, false},
                 {partition_probability, PartitionProbability},
                 {evaluation_identifier, EvaluationIdentifier}]
            )
        end,
        partitions()
    ),
    ok.

peer_to_peer_partition_overcounting_test(Config) ->
    lists:foreach(
        fun(PartitionProbability) ->
            lasp_config:set(client_number, ?CLIENTS),
            lasp_config:set(partition_probability, PartitionProbability),
            EvaluationIdentifier = list_to_atom("peer_to_peer_partition_overcounting_" ++ integer_to_list(PartitionProbability)),

            lasp_simulation_support:run(peer_to_peer_partition_overcounting_test,
                Config,
                [{mode, delta_based},
                 {simulation, ad_counter_partition_overcounting},
                 {partisan_peer_service_manager, partisan_hyparview_peer_service_manager},
                 {set, orset},
                 {broadcast, false},
                 {instrumentation, false},
                 {heavy_client, false},
                 {partition_probability, PartitionProbability},
                 {evaluation_identifier, EvaluationIdentifier}]
            )
        end,
        partitions()
    ),
    ok.

code_peer_to_peer_partition_overcounting_test(Config) ->
    lists:foreach(
        fun(PartitionProbability) ->
            lasp_config:set(client_number, ?CLIENTS),
            lasp_config:set(partition_probability, PartitionProbability),
            EvaluationIdentifier = list_to_atom("code_peer_to_peer_partition_overcounting_" ++ integer_to_list(PartitionProbability)),

            lasp_simulation_support:run(code_peer_to_peer_partition_overcounting_test,
                Config,
                [{mode, delta_based},
                 {simulation, ad_counter_partition_overcounting},
                 {partisan_peer_service_manager, partisan_hyparview_peer_service_manager},
                 {set, orset},
                 {broadcast, false},
                 {instrumentation, false},
                 {heavy_client, true},
                 {partition_probability, PartitionProbability},
                 {evaluation_identifier, EvaluationIdentifier}]
            )
        end,
        partitions()
    ),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
partitions() ->
   [0, 10, 20, 30, 40, 50, 60, 70].
