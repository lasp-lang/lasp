%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Junghun Yoo.  All Rights Reserved.
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

-module(lasp_delta_SUITE).
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

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),

    %% Start Lasp on the runner and enable instrumentation.
    lasp_support:start_runner(),

    _Config.

end_per_suite(_Config) ->
    %% Stop Lasp on the runner.
    lasp_support:stop_runner(),

    _Config.

init_per_testcase(Case, Config) ->
    Nodes = lasp_support:start_nodes(Case, Config),
    [{nodes, Nodes}|Config].

end_per_testcase(Case, Config) ->
    lasp_support:stop_nodes(Case, Config).

all() ->
    [
     normal_map_test,
     incremental_map_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

-define(SET, lasp_orset).
-define(MAX_INPUT, 10000).

%% @doc Normal map test.
normal_map_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ct:pal("Set the delta_mode: ~p", [Node]),
                        ok = rpc:call(Node, lasp_config, set, [delta_mode, false])
                  end, Nodes),

    %% Disable deltas.
    ok = lasp_config:set(delta_mode, false),

    ?assertMatch(false, lasp_config:get(delta_mode, false)),

    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, lists:seq(1,?MAX_INPUT)}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    Function = fun(X) ->
                       X * 2
               end,
    ?assertMatch(ok, lasp:map(S1, Function, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),
    ?assertEqual(lists:map(Function, lists:seq(1, ?MAX_INPUT)), ?SET:value(S2V1)),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {Time, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(lists:seq(2, ?MAX_INPUT * 2 + 4, 2), ?SET:value(S2V2)),
    lager:info("Time without incremental computation: ~p", [Time]),

    ok.

%% @doc Incremental map test.
incremental_map_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ct:pal("Set the delta_mode: ~p", [Node]),
                        ok = rpc:call(Node, lasp_config, set, [delta_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(delta_mode, true),

    ?assertMatch(true, lasp_config:get(delta_mode, false)),

    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, lists:seq(1,?MAX_INPUT)}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    Function = fun(X) ->
                       X * 2
               end,
    ?assertMatch(ok, lasp:map(S1, Function, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),
    ?assertEqual(lists:map(Function, lists:seq(1, ?MAX_INPUT)), ?SET:value(S2V1)),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {TimeInc, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(lists:seq(2, ?MAX_INPUT * 2 + 4, 2), ?SET:value(S2V2)),
    lager:info("Time with incremental computation: ~p", [TimeInc]),

    ok.
