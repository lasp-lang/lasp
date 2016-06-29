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
    %% Start Lasp on the runner and enable instrumentation.
    lasp_support:start_runner(),

    _Config.

end_per_suite(_Config) ->
    %% Stop Lasp on the runner.
    lasp_support:stop_runner(),

    _Config.

init_per_testcase(Case, Config) ->
    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:start_runner(),

    Nodes = lasp_support:start_nodes(Case, Config),
    [{nodes, Nodes}|Config].

end_per_testcase(Case, Config) ->
    lasp_support:stop_nodes(Case, Config),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:stop_runner().

all() ->
    [
     normal_map_test,
     incremental_map_test,
     normal_filter_test,
     incremental_filter_test,
     normal_union_test,
     incremental_union_test,
     normal_intersection_test,
     incremental_intersection_test,
     normal_product_test,
     incremental_product_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

-define(SET, orset).
-define(MAX_INPUT, 10).

%% @doc Normal map test.
normal_map_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, false])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Disable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, false),

    ?assertMatch(false, lasp_config:get(incremental_computation_mode, false)),

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
    ?assertEqual(sets:from_list(lists:map(Function, lists:seq(1, ?MAX_INPUT))),
                 lasp_type:query(?SET, S2V1)),

    %% Bind again.
    ?assertMatch({ok, _},
                 lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {Time, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(sets:from_list(lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
                 lasp_type:query(?SET, S2V2)),
    lager:info("Time without incremental computation: map: ~p", [Time]),

    ok.

%% @doc Incremental map test.
incremental_map_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

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
    ?assertEqual(sets:from_list(lists:map(Function, lists:seq(1, ?MAX_INPUT))),
                 lasp_type:query(?SET, S2V1)),

    %% Bind again.
    ?assertMatch({ok, _},
                 lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {TimeInc, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(sets:from_list(lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
                 lasp_type:query(?SET, S2V2)),
    lager:info("Time with incremental computation: map: ~p", [TimeInc]),

    ok.

%% @doc Normal filter test.
normal_filter_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, false])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Disable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, false),

    ?assertMatch(false, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, lists:seq(1,?MAX_INPUT)}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    Function = fun(X) ->
                       X rem 2 == 0
               end,
    ?assertMatch(ok, lasp:filter(S1, Function, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),
    ?assertEqual(sets:from_list(lists:filter(Function, lists:seq(1, ?MAX_INPUT))),
                 lasp_type:query(?SET, S2V1)),

    %% Bind again.
    ?assertMatch({ok, _},
                 lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {Time, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(sets:from_list(lists:filter(Function, lists:seq(1, ?MAX_INPUT + 2))),
                 lasp_type:query(?SET, S2V2)),
    lager:info("Time without incremental computation: filter: ~p", [Time]),

    ok.

%% @doc Incremental filter test.
incremental_filter_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, lists:seq(1,?MAX_INPUT)}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    Function = fun(X) ->
                       X rem 2 == 0
               end,
    ?assertMatch(ok, lasp:filter(S1, Function, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),
    ?assertEqual(sets:from_list(lists:filter(Function, lists:seq(1, ?MAX_INPUT))),
                 lasp_type:query(?SET, S2V1)),

    %% Bind again.
    ?assertMatch({ok, _},
                 lasp:update(S1, {add_all, [?MAX_INPUT + 1,?MAX_INPUT + 2]}, a)),

    %% Read resulting value.
    {TimeInc, {ok, {_, _, _, S2V2}}} = timer:tc(lasp, read, [S2, {strict, S2V1}]),

    ?assertEqual(sets:from_list(lists:filter(Function, lists:seq(1, ?MAX_INPUT + 2))),
                 lasp_type:query(?SET, S2V2)),
    lager:info("Time with incremental computation: filter: ~p", [TimeInc]),

    ok.

%% @doc Normal union test.
normal_union_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, false])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Disable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, false),

    ?assertMatch(false, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetUnion, _, _, _}} = lasp:declare(?SET),

    %% Apply union.
    ?assertMatch(ok, lasp:union(SetL, SetR, SetUnion)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetUnionV1}} = lasp:read(SetUnion, {strict, undefined}),
    ?assertEqual(ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                               ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetUnionV2}}} =
        timer:tc(lasp, read, [SetUnion, {strict, SetUnionV1}]),

    ?assertEqual(ordsets:union([ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2))] ++
                               [ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))] ++
                               [ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])]),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV2)))),
    lager:info("Time without incremental computation: union(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetUnionV3}}} =
        timer:tc(lasp, read, [SetUnion, {strict, SetUnionV2}]),

    ?assertEqual(ordsets:union([ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2))] ++
                               [ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))] ++
                               [ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])] ++
                               [ordsets:from_list([?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6])]),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV3)))),
    lager:info("Time without incremental computation: union(right): ~p", [TimeR]),

    ok.

%% @doc Incremental union test.
incremental_union_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetUnion, _, _, _}} = lasp:declare(?SET),

    %% Apply union.
    ?assertMatch(ok, lasp:union(SetL, SetR, SetUnion)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetUnionV1}} = lasp:read(SetUnion, {strict, undefined}),
    ?assertEqual(ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                               ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetUnionV2}}} =
        timer:tc(lasp, read, [SetUnion, {strict, SetUnionV1}]),

    ?assertEqual(ordsets:union([ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2))] ++
                               [ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))] ++
                               [ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])]),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV2)))),
    lager:info("Time with incremental computation: union(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetUnionV3}}} =
        timer:tc(lasp, read, [SetUnion, {strict, SetUnionV2}]),

    ?assertEqual(ordsets:union([ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2))] ++
                               [ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))] ++
                               [ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])] ++
                               [ordsets:from_list([?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6])]),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetUnionV3)))),
    lager:info("Time with incremental computation: union(right): ~p", [TimeR]),

    ok.

%% @doc Normal intersection test.
normal_intersection_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, false])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Disable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, false),

    ?assertMatch(false, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetIntersection, _, _, _}} = lasp:declare(?SET),

    %% Apply union.
    ?assertMatch(ok, lasp:intersection(SetL, SetR, SetIntersection)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetIntersectionV1}} = lasp:read(SetIntersection, {strict, undefined}),
    ?assertEqual(ordsets:intersection(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                      ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetIntersectionV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetIntersectionV2}}} =
        timer:tc(lasp, read, [SetIntersection, {strict, SetIntersectionV1}]),

    ?assertEqual(ordsets:intersection(
                   ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                 ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])),
                   ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetIntersectionV2)))),
    lager:info("Time without incremental computation: intersection(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetIntersectionV3}}} =
        timer:tc(lasp, read, [SetIntersection, {strict, SetIntersectionV2}]),

    ?assertEqual(ordsets:intersection(
                   ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                 ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])),
                   ordsets:union(ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3)),
                                 ordsets:from_list([?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]))),
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetIntersectionV3)))),
    lager:info("Time without incremental computation: intersection(right): ~p", [TimeR]),

    ok.

%% @doc Incremental intersection test.
incremental_intersection_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetIntersection, _, _, _}} = lasp:declare(?SET),

    %% Apply union.
    ?assertMatch(ok, lasp:intersection(SetL, SetR, SetIntersection)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetIntersectionV1}} = lasp:read(SetIntersection, {strict, undefined}),
    ?assertEqual(ordsets:intersection(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                      ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetIntersectionV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetIntersectionV2}}} =
        timer:tc(lasp, read, [SetIntersection, {strict, SetIntersectionV1}]),

    ?assertEqual(ordsets:intersection(
                   ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                 ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])),
                   ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3))),
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetIntersectionV2)))),
    lager:info("Time with incremental computation: intersection(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetIntersectionV3}}} =
        timer:tc(lasp, read, [SetIntersection, {strict, SetIntersectionV2}]),

    ?assertEqual(ordsets:intersection(
                   ordsets:union(ordsets:from_list(lists:seq(2, ?MAX_INPUT * 2, 2)),
                                 ordsets:from_list([?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4])),
                   ordsets:union(ordsets:from_list(lists:seq(3, ?MAX_INPUT * 3, 3)),
                                 ordsets:from_list([?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]))),
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetIntersectionV3)))),
    lager:info("Time with incremental computation: intersection(right): ~p", [TimeR]),

    ok.

normal_product_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to false for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, false])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Disable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, false),

    ?assertMatch(false, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetProduct, _, _, _}} = lasp:declare(?SET),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetL, SetR, SetProduct)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetProductV1}} = lasp:read(SetProduct, {strict, undefined}),
    ProductResult1 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2, 2)),
    ?assertEqual(ProductResult1,
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetProductV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetProductV2}}} =
        timer:tc(lasp, read, [SetProduct, {strict, SetProductV1}]),

    ProductResult2 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
    ?assertEqual(ProductResult2,
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetProductV2)))),
    lager:info("Time without incremental computation: product(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetProductV3}}} =
        timer:tc(lasp, read, [SetProduct, {strict, SetProductV2}]),

    ProductResult3 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3 + 6, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
    ?assertEqual(ProductResult3,
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetProductV3)))),
    lager:info("Time without incremental computation: product(right): ~p", [TimeR]),

    ok.

incremental_product_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    %% Set the delta_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set, [mode, delta_based])
                  end, Nodes),
    %% Set the incremental_computation_mode to true for all nodes.
    lists:foreach(fun(Node) ->
                        ok = rpc:call(Node, lasp_config, set,
                                      [incremental_computation_mode, true])
                  end, Nodes),

    %% Enable deltas.
    ok = lasp_config:set(mode, delta_based),

    ?assertMatch(delta_based, lasp_config:get(mode, state_based)),

    %% Enable incremental computation.
    ok = lasp_config:set(incremental_computation_mode, true),

    ?assertMatch(true, lasp_config:get(incremental_computation_mode, false)),

    %% Create initial sets.
    {ok, {SetL, _, _, _}} = lasp:declare(?SET),
    {ok, {SetR, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial left set and update.
    ?assertMatch({ok, _}, lasp:update(SetL,
                                      {add_all, lists:seq(2, ?MAX_INPUT * 2, 2)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Add elements to initial right set and update.
    ?assertMatch({ok, _}, lasp:update(SetR,
                                      {add_all, lists:seq(3, ?MAX_INPUT * 3, 3)},
                                      a)),

    %% Wait.
    timer:sleep(4000),

    %% Create resulting set.
    {ok, {SetProduct, _, _, _}} = lasp:declare(?SET),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetL, SetR, SetProduct)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetProductV1}} = lasp:read(SetProduct, {strict, undefined}),
    ProductResult1 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2, 2)),
    ?assertEqual(ProductResult1,
                 ordsets:from_list(sets:to_list(lasp_type:query(?SET, SetProductV1)))),

    %% Update the left.
    ?assertMatch({ok, _},
                 lasp:update(SetL, {add_all, [?MAX_INPUT * 2 + 2,?MAX_INPUT * 2 + 4]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeL, {ok, {_, _, _, SetProductV2}}} =
        timer:tc(lasp, read, [SetProduct, {strict, SetProductV1}]),

    ProductResult2 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
    ?assertEqual(ProductResult2,
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetProductV2)))),
    lager:info("Time with incremental computation: product(left): ~p", [TimeL]),

    %% Update the right.
    ?assertMatch({ok, _},
                 lasp:update(SetR, {add_all, [?MAX_INPUT * 3 + 3,?MAX_INPUT * 3 + 6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {TimeR, {ok, {_, _, _, SetProductV3}}} =
        timer:tc(lasp, read, [SetProduct, {strict, SetProductV2}]),

    ProductResult3 =
        lists:foldl(
          fun(Left, Product0) ->
                  lists:foldl(
                    fun(Right, Product1) ->
                            ordsets:add_element({Left, Right}, Product1)
                    end, Product0, lists:seq(3, ?MAX_INPUT * 3 + 6, 3))
          end, ordsets:new(), lists:seq(2, ?MAX_INPUT * 2 + 4, 2)),
    ?assertEqual(ProductResult3,
                 ordsets:from_list(sets:to_list(
                                     lasp_type:query(?SET, SetProductV3)))),
    lager:info("Time with incremental computation: product(right): ~p", [TimeR]),

    ok.
