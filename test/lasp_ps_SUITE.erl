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

-module(lasp_ps_SUITE).

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
    ct:pal("Beginning case: ~p", [Case]),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:start_runner(),

    Nodes = lasp_support:start_nodes(Case, Config),
    [{nodes, Nodes}|Config].

end_per_testcase(Case, Config) ->
    ct:pal("Case finished: ~p", [Case]),

    lasp_support:stop_nodes(Case, Config),

    %% Runner must start and stop in between test runs as well, to
    %% ensure that we clear the membership list (otherwise, we could
    %% delete the data on disk, but this is cleaner.)
    lasp_support:stop_runner().

all() ->
    [
        query_test,
        monotonic_read_test,
        ps_aworset_test,
        map_test,
        filter_test,
        union_test,
        product_test,
        filter_union_test,
        ps_size_t_test
    ].

-include("lasp.hrl").

%% ===================================================================
%% tests
%% ===================================================================

%% @doc Test query functionality.
query_test(_Config) ->
    %% Declare a variable.
    {ok, {SetId, _, _, _}} = lasp:declare(ps_aworset),

    %% Change it's value.
    ?assertMatch({ok, _}, lasp:update(SetId, {add, 1}, a)),

    %% Threshold read just to create a synchronization point for the
    %% value to change.
    {ok, _} = lasp:read(SetId, {strict, undefined}),

    %% Query it.
    ?assertEqual({ok, sets:from_list([1])}, lasp:query(SetId)),

    ok.

%% @doc Monotonic read.
monotonic_read_test(_Config) ->
    %% Create new set-based CRDT.
    {ok, {SetId, _, _, _}} = lasp:declare(ps_aworset),

    %% Determine my pid.
    Me = self(),

    %% Perform 3 binds, each inflation.
    ?assertMatch({ok, _}, lasp:update(SetId, {add, 1}, actor)),

    {ok, {_, _, _, V0}} = lasp:update(SetId, {add, 2}, actor),

    ?assertMatch({ok, {_, _, _, _}}, lasp:update(SetId, {add, 3}, actor)),

    %% Spawn fun which should block until lattice is strict inflation of V0.
    I1 = first_read,
    spawn(fun() -> Me ! {I1, lasp:read(SetId, {strict, V0})} end),

    %% Ensure we receive [1, 2, 3].
    Set1 = receive
               {I1, {ok, {_, _, _, X}}} ->
                   lasp_type:query(ps_aworset, X)
           end,

    %% Perform more inflation.
    {ok, {_, _, _, V1}} = lasp:update(SetId, {add, 4}, actor),

    ?assertMatch({ok, _}, lasp:update(SetId, {add, 5}, actor)),

    %% Spawn fun which should block until lattice is strict inflation of V1.
    I2 = second_read,
    spawn(fun() -> Me ! {I2, lasp:read(SetId, {strict, V1})} end),

    %% Ensure we receive [1, 2, 3, 4, 5].
    Set2 = receive
               {I2, {ok, {_, _, _, Y}}} ->
                   lasp_type:query(ps_aworset, Y)
           end,

    ?assertEqual(
        {sets:from_list([1,2,3]), sets:from_list([1,2,3,4,5])}, {Set1, Set2}),

    ok.

%% @doc Test of the orset with ps.
ps_aworset_test(_Config) ->
    {ok, {L1, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {L2, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {L3, _, _, _}} = lasp:declare(ps_aworset),

    %% Attempt pre, and post- dataflow variable bind operations.
    ?assertMatch(ok, lasp:bind_to(L2, L1)),
    {ok, {_, _, _, S2}} = lasp:update(L1, {add, 1}, a),
    ?assertMatch(ok, lasp:bind_to(L3, L1)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S1}} = lasp:read(L3, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L2, {strict, undefined}),
    {ok, {_, _, _, S1}} = lasp:read(L1, {strict, undefined}),

    Self = self(),

    spawn_link(
        fun() ->
            {ok, _} = lasp:wait_needed(L1, {strict, S1}),
            Self ! threshold_met
        end),

    ?assertMatch({ok, _}, lasp:bind(L1, S2)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S2L3}} = lasp:read(L3, {strict, undefined}),
    ?assertEqual(S2L3, lasp_type:merge(ps_aworset, S2, S2L3)),
    {ok, {_, _, _, S2L2}} = lasp:read(L2, {strict, undefined}),
    ?assertEqual(S2L2, lasp_type:merge(ps_aworset, S2, S2L2)),
    {ok, {_, _, _, S2L1}} = lasp:read(L1, {strict, undefined}),
    ?assertEqual(S2L1, lasp_type:merge(ps_aworset, S2, S2L1)),

    %% Read at the S2 threshold level.
    {ok, {_, _, _, _}} = lasp:read(L1, S2),

    %% Wait for wait_needed to unblock.
    receive
        threshold_met ->
            ok
    end,

    {ok, {L5, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {L6, _, _, _}} = lasp:declare(ps_aworset),

    spawn_link(
        fun() ->
            {ok, _} =
                lasp:read_any(
                    [{L5, {strict, undefined}}, {L6, {strict, undefined}}]),
            Self ! read_any
        end),

    {ok, _} = lasp:update(L5, {add, 1}, a),

    receive
        read_any ->
            ok
    end.

%% @doc Map operation test.
map_test(_Config) ->
    %% Create initial set.
    {ok, {SetM1, _, _, _}} = lasp:declare(ps_aworset),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 1}, a)),
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 2}, a)),
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 3}, a)),

    %% Create second set.
    {ok, {SetM2, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply map.
    ?assertMatch(ok, lasp:map(SetM1, fun(X) -> X * 2 end, SetM2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 4}, a)),
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 5}, a)),
    ?assertMatch({ok, _}, lasp:update(SetM1, {add, 6}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetM1V}} = lasp:read(SetM1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, SetM2V}} = lasp:read(SetM2, {strict, undefined}),

    ?assertEqual(
        {ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6,8,10,12])},
        {ok, lasp_type:query(ps_aworset, SetM1V), lasp_type:query(ps_aworset, SetM2V)}),

    ok.

%% @doc Filter operation test.
filter_test(_Config) ->
    %% Create initial set.
    {ok, {SetF1, _, _, _}} = lasp:declare(ps_aworset),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 1}, a)),
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 2}, a)),
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 3}, a)),

    %% Create second set.
    {ok, {SetF2, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply filter.
    ?assertMatch(ok, lasp:filter(SetF1, fun(X) -> X rem 2 == 0 end, SetF2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 4}, a)),
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 5}, a)),
    ?assertMatch({ok, _}, lasp:update(SetF1, {add, 6}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetF1V}} = lasp:read(SetF1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, SetF2V}} = lasp:read(SetF2, {strict, undefined}),

    ?assertEqual(
        {ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6])},
        {ok, lasp_type:query(ps_aworset, SetF1V), lasp_type:query(ps_aworset, SetF2V)}),

    ok.

%% @doc Union operation test.
union_test(_Config) ->
    %% Create initial sets.
    {ok, {SetU1, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {SetU2, _, _, _}} = lasp:declare(ps_aworset),

    %% Create output set.
    {ok, {SetU3, _, _, _}} = lasp:declare(ps_aworset),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(SetU1, {add, 1}, a)),
    ?assertMatch({ok, _}, lasp:update(SetU1, {add, 2}, a)),
    ?assertMatch({ok, _}, lasp:update(SetU1, {add, 3}, a)),
    ?assertMatch({ok, _}, lasp:update(SetU2, {add, a}, a)),
    ?assertMatch({ok, _}, lasp:update(SetU2, {add, b}, a)),
    ?assertMatch({ok, _}, lasp:update(SetU2, {add, c}, a)),

    %% Apply union.
    ?assertMatch(ok, lasp:union(SetU1, SetU2, SetU3)),

    %% Sleep.
    timer:sleep(400),

    %% Read union.
    {ok, {_, _, _, Union0}} = lasp:read(SetU3, undefined),

    %% Read union value.
    Union = lasp_type:query(ps_aworset, Union0),

    ?assertEqual({ok, sets:from_list([1,2,3,a,b,c])}, {ok, Union}),

    ok.

%% @doc Cartesian product test.
product_test(_Config) ->
    %% Create initial sets.
    {ok, {SetP1, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {SetP2, _, _, _}} = lasp:declare(ps_aworset),

    %% Create output set.
    {ok, {SetP3, _, _, _}} = lasp:declare(ps_aworset),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(SetP1, {add, 1}, a)),
    ?assertMatch({ok, _}, lasp:update(SetP1, {add, 2}, a)),
    ?assertMatch({ok, _}, lasp:update(SetP1, {add, 3}, a)),
    ?assertMatch({ok, _}, lasp:update(SetP2, {add, a}, a)),
    ?assertMatch({ok, _}, lasp:update(SetP2, {add, b}, a)),
    ?assertMatch({ok, _}, lasp:update(SetP2, {add, 3}, a)),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetP1, SetP2, SetP3)),

    %% Sleep.
    timer:sleep(400),

    %% Read product.
    {ok, {_, _, _, Product0}} = lasp:read(SetP3, undefined),

    %% Read product value.
    Product = lasp_type:query(ps_aworset, Product0),

    ?assertEqual(
        {ok, sets:from_list([{1,3},{1,a},{1,b},{2,3},{2,a},{2,b},{3,3},{3,a},{3,b}])},
        {ok, Product}),

    ok.

%% @doc Filter & Union operation test.
filter_union_test(_Config) ->
    %% Create initial set.
    {ok, {SetInput, _, _, _}} = lasp:declare(ps_aworset),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(SetInput, {add, 1}, a)),
    ?assertMatch({ok, _}, lasp:update(SetInput, {add, 2}, a)),
    ?assertMatch({ok, _}, lasp:update(SetInput, {add, 3}, a)),
    ?assertMatch({ok, _}, lasp:update(SetInput, {add, 4}, a)),

    %% Create filter result set.
    {ok, {SetFilter, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply filter.
    ?assertMatch(
        ok, lasp:filter(SetInput, fun(X) -> X rem 2 == 1 end, SetFilter)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetFilterV}} = lasp:read(SetFilter, {strict, undefined}),

    ?assertEqual(
        {ok, sets:from_list([1,3])},
        {ok, lasp_type:query(ps_aworset, SetFilterV)}),

    %% Create union result set.
    {ok, {SetUnion, _, _, _}} = lasp:declare(ps_aworset),

    %% Apply union.
    ?assertMatch(
        ok, lasp:union(SetFilter, SetInput, SetUnion)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, SetUnionV}} = lasp:read(SetUnion, {strict, undefined}),

    ?assertEqual(
        {ok, sets:from_list([1,2,3,4])},
        {ok, lasp_type:query(ps_aworset, SetUnionV)}),

    ok.

ps_size_t_test(_Config) ->
    {ok, {SetA, _, _, _}} = lasp:declare(ps_aworset),
    {ok, {SetB, _, _, _}} = lasp:declare(ps_aworset),

    {ok, {SetProduct, _, _, _}} = lasp:declare(ps_aworset),

    ?assertMatch({ok, _}, lasp:update(SetA, {add, a}, a)),
    ?assertMatch({ok, _}, lasp:update(SetB, {add, 1}, a)),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetA, SetB, SetProduct)),

    %% Sleep.
    timer:sleep(400),

    {ok, {SizeTProduct, _, _, _}} = lasp:declare(ps_size_t),

    ?assertMatch(ok, lasp:length(SetProduct, SizeTProduct)),

    %% Sleep.
    timer:sleep(400),

    %% Read.
    {ok, {_, _, _, SizeTProduct0}} = lasp:read(SizeTProduct, undefined),

    %% Read value.
    SizeTProductV0 = lasp_type:query(ps_size_t, SizeTProduct0),

    ?assertEqual({ok, 1}, {ok, SizeTProductV0}),

    ?assertMatch({ok, _}, lasp:update(SetB, {rmv, 1}, a)),

    %% Sleep.
    timer:sleep(400),

    %% Read.
    {ok, {_, _, _, SizeTProduct1}} = lasp:read(SizeTProduct, SizeTProduct0),

    %% Read value.
    SizeTProductV1 = lasp_type:query(ps_size_t, SizeTProduct1),

    ?assertEqual({ok, 0}, {ok, SizeTProductV1}),

    ok.
