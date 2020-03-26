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

-module(lasp_peer_to_peer_consistent_group_rank_SUITE).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

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
-include("lasp_ext_group_rank.hrl").

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
        peer_to_peer_state_based_ext_v1_test,
        peer_to_peer_state_based_ext_v2_test,
        peer_to_peer_state_based_ext_v3_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

default_test(_Config) ->
    ok.

%% ===================================================================
%% peer-to-peer
%% ===================================================================

peer_to_peer_state_based_ext_v1_test(Config) ->
    lasp_simulation_support:run(
        peer_to_peer_consistent_group_rank_state_based_ext_v1_test,
        Config,
        [
            {mode, state_based},
            {simulation, consistent_group_rank},
            {
                partisan_peer_service_manager,
                partisan_hyparview_peer_service_manager},
            {set, orset},
            {ext_type_version, ext_type_orset_base_v1},
            {group_rank_input_size, 9},
            {jitter, true},
            {jitter_percent, 50},
            {broadcast, false},
            {evaluation_identifier, peer_to_peer_state_based_v1},
            {eval_id, peer_to_peer_state_based_v1}]),
    ok.

peer_to_peer_state_based_ext_v2_test(Config) ->
    lasp_simulation_support:run(
        peer_to_peer_consistent_group_rank_state_based_ext_v2_test,
        Config,
        [
            {mode, state_based},
            {simulation, consistent_group_rank},
            {
                partisan_peer_service_manager,
                partisan_hyparview_peer_service_manager},
            {set, orset},
            {ext_type_version, ext_type_orset_base_v2},
            {group_rank_input_size, 9},
            {jitter, true},
            {jitter_percent, 50},
            {broadcast, false},
            {evaluation_identifier, peer_to_peer_state_based_v2},
            {eval_id, peer_to_peer_state_based_v2}]),
    ok.

peer_to_peer_state_based_ext_v3_test(Config) ->
    lasp_simulation_support:run(
        peer_to_peer_consistent_group_rank_state_based_ext_v3_test,
        Config,
        [
            {mode, state_based},
            {simulation, consistent_group_rank},
            {
                partisan_peer_service_manager,
                partisan_hyparview_peer_service_manager},
            {set, orset},
            {ext_type_version, ext_type_orset_base_v3},
            {group_rank_input_size, 9},
            {jitter, true},
            {jitter_percent, 50},
            {broadcast, false},
            {evaluation_identifier, peer_to_peer_state_based_v3},
            {eval_id, peer_to_peer_state_based_v3}]),
    ok.
