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

-module(lasp_ext_SUITE).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

%% common_test callbacks
-export([
    %% suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-compile([export_all]).

-include("lasp.hrl").
-include("lasp_ext.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    lasp_config:set(ext_type_version, ext_type_orset_base_v5),
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
        query_ext_test,
        lww_register_test,
        map_test,
        filter_test,
        product_test,
        query_ext_consistent_test,
        set_count_test,
        group_by_sum_test,
        order_by_test,
        order_by_after_group_by_test,
        group_rank_test,
        group_rank_query_ext_consistent_test,
        group_rank_query_ext_consistent_complex_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

%% @doc Test query_ext functionality.
query_ext_test(_Config) ->
    %% Declare a variable.
    SetAllPathInfoList = [],
    InputSetId = <<"inputset">>,
    {ok, {Set, _, _, _}} =
        lasp:declare(
            InputSetId,
            {?EXT_AWORSET_INPUT_TYPE, [SetAllPathInfoList, {undefined, undefined}]}),

    %% Change it's value.
    {ok, _} = lasp:update(Set, {add, 2}, "a"),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(Set, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [2].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, SetV1}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INPUT_TYPE, {InitSubset, SetV1}),
                    SetV1}
        end,

    %% Perform more inflation.
    {ok, _} = lasp:update(Set, {add, 3}, "a"),

    %% Spawn fun which should block until lattice is strict inflation of
    %% Set1.
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(Set, {strict, ReadValue1})}
        end),

    %% Ensure we receive [2, 3].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, SetV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INPUT_TYPE, {Subset1, SetV2}),
                    SetV2}
        end,

    %% Query it.
    ?assertEqual(
        {ok, sets:from_list([2]), sets:from_list([2, 3])},
        {ok, ReadResult1, ReadResult2}),

    ok.

%% @doc LWW Register test.
lww_register_test(_Config) ->
    %% Create initial set.
    RegisterAllPathInfoList = [],
    InputRegisterId = <<"inputregister">>,
    {ok, {Register, _, _, _}} =
        lasp:declare(
            InputRegisterId,
            {?EXT_LWWREGISTER_INPUT_TYPE, [RegisterAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(Register, {write, 7}, "a")),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(Register, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [7].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, RegisterV1}}} ->
                {
                    lasp_type:query_ext(?EXT_LWWREGISTER_INPUT_TYPE, {InitSubset, RegisterV1}),
                    RegisterV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(Register, {write, 3}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(Register, {strict, ReadValue1})}
        end),

    %% Ensure we receive [3].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, RegisterV2}}} ->
                {
                    lasp_type:query_ext(?EXT_LWWREGISTER_INPUT_TYPE, {Subset1, RegisterV2}),
                    RegisterV2}
        end,

    ?assertEqual(
        {ok, sets:from_list([7]), sets:from_list([3])},
        {ok, ReadResult1, ReadResult2}),

    ok.

%% @doc Map operation test.
map_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputSetId = <<"inputset">>,
    {ok, {{InputSetId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputSetId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 1}, "a")),

    %% Create second set.
    IntermediateSet1AllPathInfoList = [{InputSetId, {undefined, undefined}}],
    IntermediateSet1Id = <<"intermediateset1">>,
    {ok, {{IntermediateSet1Id, _}=IntermediateSet1, _, _, _}} =
        lasp:declare(
            IntermediateSet1Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet1AllPathInfoList, {InputSetId, undefined}]}),

    %% Apply map.
    ?assertMatch(ok, lasp:map(InputSet, fun(X) -> X * 2 end, IntermediateSet1)),

    %% Create third set.
    IntermediateSet2AllPathInfoList =
        [
            {IntermediateSet1Id, {InputSetId, undefined}},
            {InputSetId, {undefined, undefined}}],
    IntermediateSet2Id = <<"intermediateset2">>,
    {ok, {IntermediateSet2, _, _, _}} =
        lasp:declare(
            IntermediateSet2Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet2AllPathInfoList, {IntermediateSet1Id, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(IntermediateSet1, fun(X) -> X * 3 end, IntermediateSet2)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(IntermediateSet2, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [6].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, IntermediateSet2V1}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, IntermediateSet2V1}),
                    IntermediateSet2V1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 2}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(IntermediateSet2, {strict, ReadValue1})}
        end),

    %% Ensure we receive [6, 12].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, IntermediateSet2V2}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, IntermediateSet2V2}),
                    IntermediateSet2V2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, IntermediateSet1V1}} =
        lasp:read(IntermediateSet1, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([1, 2]),
            sets:from_list([2, 4]),
            sets:from_list([6]),
            sets:from_list([6, 12])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, IntermediateSet1V1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc Filter operation test.
filter_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputSetId = <<"inputset">>,
    {ok, {{InputSetId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputSetId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 1}, "a")),

    %% Create second set.
    IntermediateSet1AllPathInfoList = [{InputSetId, {undefined, undefined}}],
    IntermediateSet1Id = <<"intermediateset1">>,
    {ok, {{IntermediateSet1Id, _}=IntermediateSet1, _, _, _}} =
        lasp:declare(
            IntermediateSet1Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet1AllPathInfoList, {InputSetId, undefined}]}),

    %% Apply filter.
    ?assertMatch(
        ok, lasp:filter(InputSet, fun(X) -> X /= 1 end, IntermediateSet1)),

    %% Create third set.
    IntermediateSet2AllPathInfoList =
        [
            {IntermediateSet1Id, {InputSetId, undefined}},
            {InputSetId, {undefined, undefined}}],
    IntermediateSet2Id = <<"intermediateset2">>,
    {ok, {IntermediateSet2, _, _, _}} =
        lasp:declare(
            IntermediateSet2Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet2AllPathInfoList, {IntermediateSet1Id, undefined}]}),

    %% Apply filter.
    ?assertMatch(
        ok,
        lasp:filter(IntermediateSet1, fun(X) -> X /= 2 end, IntermediateSet2)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(IntermediateSet2, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, IntermediateSet2V1}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, IntermediateSet2V1}),
                    IntermediateSet2V1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 2}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(IntermediateSet2, {strict, ReadValue1})}
        end),

    %% Ensure we receive [].
    {{Subset2, ReadResult2}, ReadValue2} =
        receive
            {I2, {ok, {_, _, _, IntermediateSet2V2}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, IntermediateSet2V2}),
                    IntermediateSet2V2}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 3}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I3 = third_read,
    spawn(
        fun() ->
            Me ! {I3, lasp:read(IntermediateSet2, {strict, ReadValue2})}
        end),

    %% Ensure we receive [3].
    {{_Subset3, ReadResult3}, _ReadValue3} =
        receive
            {I3, {ok, {_, _, _, IntermediateSet2V3}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset2, IntermediateSet2V3}),
                    IntermediateSet2V3}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, IntermediateSet1V1}} =
        lasp:read(IntermediateSet1, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([1, 2, 3]),
            sets:from_list([2, 3]),
            sets:new(),
            sets:new(),
            sets:from_list([3])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, IntermediateSet1V1),
            ReadResult1,
            ReadResult2,
            ReadResult3}),

    ok.

%% @doc Product operation test.
product_test(_Config) ->
    %% Create initial sets.
    InputSet1AllPathInfoList = [],
    InputSet1Id = <<"inputset1">>,
    InputSet2Id = <<"inputset2">>,
    InputSet3Id = <<"inputset3">>,
    {ok, {{InputSet1Id, _}=InputSet1, _, _, _}} =
        lasp:declare(
            InputSet1Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet1AllPathInfoList, {undefined, undefined}]}),
    InputSet2AllPathInfoList = [],
    {ok, {{InputSet2Id, _}=InputSet2, _, _, _}} =
        lasp:declare(
            InputSet2Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet2AllPathInfoList, {undefined, undefined}]}),
    InputSet3AllPathInfoList = [],
    {ok, {{InputSet3Id, _}=InputSet3, _, _, _}} =
        lasp:declare(
            InputSet3Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet3AllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial sets and update.
    ?assertMatch({ok, _}, lasp:update(InputSet1, {add, 1}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSet2, {add, 2}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSet3, {add, 3}, "a")),

    %% Create an intermediate set.
    IntermediateSet1AllPathInfoList =
        [
            {InputSet1Id, {undefined, undefined}},
            {InputSet2Id, {undefined, undefined}}],
    IntermediateSet1Id = <<"intermediateset1">>,
    {ok, {{IntermediateSet1Id, _}=IntermediateSet1, _, _, _}} =
        lasp:declare(
            IntermediateSet1Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet1AllPathInfoList, {InputSet1Id, InputSet2Id}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(InputSet1, InputSet2, IntermediateSet1)),

    %% Create another intermediate set.
    IntermediateSet2AllPathInfoList =
        [
            {InputSet3Id, {undefined, undefined}},
            {IntermediateSet1Id, {InputSet1Id, InputSet2Id}},
            {InputSet1Id, {undefined, undefined}},
            {InputSet2Id, {undefined, undefined}}],
    IntermediateSet2Id = <<"intermediateset1">>,
    {ok, {IntermediateSet2, _, _, _}} =
        lasp:declare(
            IntermediateSet2Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet2AllPathInfoList, {InputSet3Id, IntermediateSet1Id}]}),

    %% Apply product.
    ?assertMatch(
        ok, lasp:product(InputSet3, IntermediateSet1, IntermediateSet2)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(IntermediateSet2, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [{3, {1, 2}}].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, IntermediateSet2V1}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, IntermediateSet2V1}),
                    IntermediateSet2V1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet1, {add, 4}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(IntermediateSet2, {strict, ReadValue1})}
        end),

    %% Ensure we receive [{3, {1, 2}}, {3, {4, 2}}].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, IntermediateSet2V2}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, IntermediateSet2V2}),
                    IntermediateSet2V2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSet1V1}} = lasp:read(InputSet1, {strict, undefined}),
    {ok, {_, _, _, InputSet2V1}} = lasp:read(InputSet2, {strict, undefined}),
    {ok, {_, _, _, InputSet3V1}} = lasp:read(InputSet3, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, IntermediateSet1V1}} =
        lasp:read(IntermediateSet1, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([1, 4]),
            sets:from_list([2]),
            sets:from_list([3]),
            sets:from_list([{1, 2}, {4, 2}]),
            sets:from_list([{3, {1, 2}}]),
            sets:from_list([{3, {1, 2}}, {3, {4, 2}}])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet1V1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet2V1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet3V1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, IntermediateSet1V1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc query_ext_consistent() functionality test.
query_ext_consistent_test(_Config) ->
    %% Create initial sets.
    InputSet1AllPathInfoList = [],
    Input1Id = <<"input1">>,
    {ok, {{Input1Id, _}=InputSet1, _, _, _}} =
        lasp:declare(
            Input1Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet1AllPathInfoList, {undefined, undefined}]}),
    InputSet2AllPathInfoList = [],
    Input2Id = <<"input2">>,
    {ok, {{Input2Id, _}=InputSet2, _, _, _}} =
        lasp:declare(
            Input2Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet2AllPathInfoList, {undefined, undefined}]}),
    InputSet3AllPathInfoList = [],
    Input3Id = <<"input3">>,
    {ok, {{Input3Id, _}=InputSet3, _, _, _}} =
        lasp:declare(
            Input3Id,
            {?EXT_AWORSET_INPUT_TYPE, [InputSet3AllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial sets and update.
    ?assertMatch({ok, _}, lasp:update(InputSet1, {add, 1}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSet2, {add, 2}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSet3, {add, 3}, "a")),

    %% Create an intermediate set.
    IntermediateSet1AllPathInfoList =
        [
            {Input1Id, {undefined, undefined}},
            {Input2Id, {undefined, undefined}}],
    Inter1Id = <<"intermediate1">>,
    {ok, {{Inter1Id, _}=IntermediateSet1, _, _, _}} =
        lasp:declare(
            Inter1Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet1AllPathInfoList, {Input1Id, Input2Id}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(InputSet1, InputSet2, IntermediateSet1)),

    %% Create another intermediate set.
    IntermediateSet2AllPathInfoList =
        [
            {Input2Id, {undefined, undefined}},
            {Input3Id, {undefined, undefined}}],
    Inter2Id = <<"intermediate2">>,
    {ok, {{Inter2Id, _}=IntermediateSet2, _, _, _}} =
        lasp:declare(
            Inter2Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet2AllPathInfoList, {Input2Id, Input3Id}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(InputSet2, InputSet3, IntermediateSet2)),

    %% Create another intermediate set.
    IntermediateSet3AllPathInfoList =
        [
            {Inter1Id, {Input1Id, Input2Id}},
            {Input1Id, {undefined, undefined}},
            {Input2Id, {undefined, undefined}},
            {Inter2Id, {Input2Id, Input3Id}},
            {Input2Id, {undefined, undefined}},
            {Input3Id, {undefined, undefined}}],
    Inter3Id = <<"intermediate3">>,
    {ok, {{Inter3Id, _}=IntermediateSet3, _, _, _}} =
        lasp:declare(
            Inter3Id,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [IntermediateSet3AllPathInfoList, {Inter1Id, Inter2Id}]}),

    %% Apply product.
    ?assertMatch(
        ok, lasp:product(IntermediateSet1, IntermediateSet2, IntermediateSet3)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(IntermediateSet3, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    InitCDS = ordsets:new(),
    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{NewSubset1, NewCDS1, ConsistentReadResult1}, ConsistentReadValue1} =
        receive
            {I1, {ok, {_, _, _, IntermediateSet3V1}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE,
                        {InitSubset, InitCDS, IntermediateSet3V1}),
                    IntermediateSet3V1}
        end,

    %% Perform more inflation.
    ?assertMatch({ok, _}, lasp:update(InputSet2, {add, 4}, "a")),

    %% Spawn fun which should block until lattice is a strict inflation
    %% of IntermediateSet3V1.
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(IntermediateSet3, {strict, ConsistentReadValue1})}
        end),

    %% Ensure we receive
    %% [{{1, 2}, {2, 3}}, {{1, 2}, {4, 3}}, {{1, 4}, {2, 3}}, {{1, 4}, {4, 3}}].
    {{NewSubset2, NewCDS2, ConsistentReadResult2}, ConsistentReadValue2} =
        receive
            {I2, {ok, {_, _, _, IntermediateSet3V2}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE,
                        {NewSubset1, NewCDS1, IntermediateSet3V2}),
                    IntermediateSet3V2}
        end,

    %% Spawn fun which should block until lattice is a strict inflation
    %% of IntermediateSet3V1.
    I3 = third_read,
    spawn(
        fun() ->
            Me ! {I3, lasp:read(IntermediateSet3, {strict, ConsistentReadValue2})}
        end),

    %% Ensure we receive
    %% [{{1, 2}, {2, 3}}, {{1, 2}, {4, 3}}, {{1, 4}, {2, 3}}, {{1, 4}, {4, 3}}].
    {{_NewSubset3, _NewCDS3, ConsistentReadResult3}, _ConsistentReadValue3} =
        receive
            {I3, {ok, {_, _, _, IntermediateSet3V3}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE,
                        {NewSubset2, NewCDS2, IntermediateSet3V3}),
                    IntermediateSet3V3}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSet1V1}} = lasp:read(InputSet1, {strict, undefined}),
    %% Read resulting value.
    {ok, {_, _, _, InputSet2V1}} = lasp:read(InputSet2, {strict, undefined}),
    %% Read resulting value.
    {ok, {_, _, _, InputSet3V1}} = lasp:read(InputSet3, {strict, undefined}),
    %% Read resulting value.
    {ok, {_, _, _, IntermediateSet1V1}} =
        lasp:read(IntermediateSet1, {strict, undefined}),
    %% Read resulting value.
    {ok, {_, _, _, IntermediateSet2V1}} =
        lasp:read(IntermediateSet2, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([1]),
            sets:from_list([2, 4]),
            sets:from_list([3]),
            sets:from_list([{1, 2}, {1, 4}]),
            sets:from_list([{2, 3}, {4, 3}]),
            sets:from_list([{{1, 2}, {2, 3}}]),
            sets:from_list([{{1, 2}, {2, 3}}]),
            sets:from_list(
                [
                    {{1, 2}, {2, 3}},
                    {{1, 4}, {4, 3}}])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet1V1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet2V1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSet3V1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, IntermediateSet1V1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, IntermediateSet2V1),
            ConsistentReadResult1,
            ConsistentReadResult2,
            ConsistentReadResult3}),

    ok.

%% @doc Set count operation test.
set_count_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputId = <<"input">>,
    {ok, {{InputId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 1}, "a")),

    AllPathInfoList0 =
        [{InputId, {undefined, undefined}}],
    CountId = <<"count">>,
    %% Create set_count object.
    {ok, {{CountId, _}=SetCount, _, _, _}} =
        lasp:declare(
            CountId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InputId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(ok, lasp:set_count(InputSet, SetCount)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(SetCount, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [1].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, SetCountV1}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {InitSubset, SetCountV1}),
                    SetCountV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 2}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(SetCount, {strict, ReadValue1})}
        end),

    %% Ensure we receive [2].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, SetCountV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {Subset1, SetCountV2}),
                    SetCountV2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([1, 2]),
            sets:from_list([1]),
            sets:from_list([2])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc Group-by sum operation test.
group_by_sum_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputId = <<"input">>,
    {ok, {{InputId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, {a, 3}}, "a")),

    AllPathInfoList0 =
        [{InputId, {undefined, undefined}}],
    GSumId = <<"groupbysum">>,
    %% Create set_count object.
    {ok, {{GSumId, _}=GroupBySum, _, _, _}} =
        lasp:declare(
            GSumId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InputId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:group_by_sum(
            InputSet,
            fun(undefined, ElemR) ->
                ElemR;
                (ElemL, ElemR) ->
                    ElemL + ElemR
            end,
            GroupBySum)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(GroupBySum, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [{a, 3}].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, GroupBySumV1}}} ->
                {
                    lasp_type:query_ext(
                        ?EXT_AWORSET_AGGRESULT_TYPE, {InitSubset, GroupBySumV1}),
                    GroupBySumV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, {a, 4}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(GroupBySum, {strict, ReadValue1})}
        end),

    %% Ensure we receive [{a, 7}]].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, GroupBySumV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {Subset1, GroupBySumV2}),
                    GroupBySumV2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([{a, 3}, {a, 4}]),
            sets:from_list([{a, 3}]),
            sets:from_list([{a, 7}])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc Order by operation test.
order_by_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputId = <<"input">>,
    {ok, {{InputId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 7}, "a")),

    AllPathInfoList0 =
        [{InputId, {undefined, undefined}}],
    OSetId = <<"orderedset">>,
    %% Create order_by object.
    {ok, {{OSetId, _}=OrderedSet, _, _, _}} =
        lasp:declare(
            OSetId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InputId, undefined}]}),

    %% Apply order_by.
    ?assertMatch(
        ok,
        lasp:order_by(
            InputSet,
            fun(ElemL, ElemR) ->
                ElemL =< ElemR
            end,
            OrderedSet)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(OrderedSet, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [[7]].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, OrderedSetV1}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {InitSubset, OrderedSetV1}),
                    OrderedSetV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, 3}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(OrderedSet, {strict, ReadValue1})}
        end),

    %% Ensure we receive [[3, 7]].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, OrderedSetV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {Subset1, OrderedSetV2}),
                    OrderedSetV2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([3, 7]),
            sets:from_list([[7]]),
            sets:from_list([[3, 7]])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc Order by operation after Group by operation test.
order_by_after_group_by_test(_Config) ->
    %% Create initial set.
    InputSetAllPathInfoList = [],
    InputId = <<"input">>,
    {ok, {{InputId, _}=InputSet, _, _, _}} =
        lasp:declare(
            InputId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, {a, 3}}, "a")),

    AllPathInfoList0 =
        [{InputId, {undefined, undefined}}],
    GSumId = <<"groupbysum">>,
    %% Create set_count object.
    {ok, {{GSumId, _}=GroupBySum, _, _, _}} =
        lasp:declare(
            GSumId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InputId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:group_by_sum(
            InputSet,
            fun(undefined, ElemR) ->
                ElemR;
                (ElemL, ElemR) ->
                    ElemL + ElemR
            end,
            GroupBySum)),

    AllPathInfoList1 =
        [
            {GSumId, {InputId, undefined}},
            {InputId, {undefined, undefined}}],
    OSetId = <<"orderedset">>,
    %% Create order_by object.
    {ok, {{OSetId, _}=OrderedSet, _, _, _}} =
        lasp:declare(
            OSetId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList1, {GSumId, undefined}]}),

    %% Apply order_by.
    ?assertMatch(
        ok,
        lasp:order_by(
            GroupBySum,
            fun({_FstL, SndL}=_ElemL, {_FstR, SndR}=_ElemR) ->
                SndL =< SndR
            end,
            OrderedSet)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(OrderedSet, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [[{a, 3}]].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, OrderedSetV1}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {InitSubset, OrderedSetV1}),
                    OrderedSetV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSet, {add, {a, 4}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(OrderedSet, {strict, ReadValue1})}
        end),

    %% Ensure we receive [[{a, 7}]].
    {{_Subset2, ReadResult2}, _ReadValue2} =
        receive
            {I2, {ok, {_, _, _, OrderedSetV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_AGGRESULT_TYPE, {Subset1, OrderedSetV2}),
                    OrderedSetV2}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetV1}} = lasp:read(InputSet, {strict, undefined}),

    {ok, {_, _, _, GroupBySumV1}} = lasp:read(GroupBySum, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([{a, 3}, {a, 4}]),
            sets:from_list([{a, 7}]),
            sets:from_list([[{a, 3}]]),
            sets:from_list([[{a, 7}]])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, GroupBySumV1),
            ReadResult1,
            ReadResult2}),

    ok.

%% @doc Group rank unit test.
group_rank_test(_Config) ->
    Group1 = ordsets:from_list([user_1, user_2]),
    Group2 = ordsets:from_list([user_2]),

    %% Create initial sets.
    InputSetAllPathInfoList = [],
    InputSetUserId = <<"u">>,
    {ok, {{InputSetUserId, _}=InputSetUser, _, _, _}} =
        lasp:declare(
            InputSetUserId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputSetGroupId = <<"g">>,
    {ok, {{InputSetGroupId, _}=InputSetGroup, _, _, _}} =
        lasp:declare(
            InputSetGroupId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputRegisterDividerId = <<"d">>,
    {ok, {{InputRegisterDividerId, _}=InputRegisterDivider, _, _, _}} =
        lasp:declare(
            InputRegisterDividerId,
            {?EXT_LWWREGISTER_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial sets and update.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_1, 1000}}, "a")),
    ?assertMatch(
        {ok, _}, lasp:update(InputSetGroup, {add, {group_1, Group1}}, "a")),
    ?assertMatch(
        {ok, _}, lasp:update(InputRegisterDivider, {write, [50, 50]}, "a")),

    %% Create an intermediate set.
    InterSetGroupXUserAllPathInfoList =
        [
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserId = <<"gXu">>,
    {ok, {{InterSetGroupXUserId, _}=InterSetGroupXUser, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserAllPathInfoList, {InputSetGroupId, InputSetUserId}]}),

    %% Apply product.
    ?assertMatch(
        ok, lasp:product(InputSetGroup, InputSetUser, InterSetGroupXUser)),

    %% Create an intermediate set.
    InterSetGroupXUserMatchAllPathInfoList =
        [
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserMatchId = <<"gXuF">>,
    {ok, {{InterSetGroupXUserMatchId, _}=InterSetGroupXUserMatch, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserMatchId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserMatchAllPathInfoList, {InterSetGroupXUserId, undefined}]}),

    %% Apply filter.
    ?assertMatch(
        ok,
        lasp:filter(
            InterSetGroupXUser,
            fun({{_GroupId, UserIdSet}, {UserId, _UserPoints}}) ->
                ordsets:is_element(UserId, UserIdSet)
            end,
            InterSetGroupXUserMatch)),

    AllPathInfoList0 =
        [
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    GroupBySumGroupXPointsId = <<"gXuFG">>,
    %% Create group_by_sum object.
    {ok, {{GroupBySumGroupXPointsId, _}=GroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            GroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InterSetGroupXUserMatchId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:group_by_sum(
            InterSetGroupXUserMatch,
            fun(undefined, {UserIdR, UserPointsR}) ->
                {
                    ordsets:add_element(UserIdR, ordsets:new()),
                    0 + UserPointsR};
            ({UserIdL, UserPointsL}, {UserIdR, UserPointsR}) ->
                {
                    ordsets:add_element(UserIdR, UserIdL),
                    UserPointsL + UserPointsR}
            end,
            GroupBySumGroupXPoints)),

    AllPathInfoList1 =
        [
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    OrderedGroupBySumGroupXPointsId = <<"gXuFGO">>,
    %% Create order_by object.
    {ok, {{OrderedGroupBySumGroupXPointsId, _}=OrderedGroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            OrderedGroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList1, {GroupBySumGroupXPointsId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:order_by(
            GroupBySumGroupXPoints,
            fun({
                    {_GroupIdL, _GroupUsersL},
                    {_GroupUsersSumedL, GroupPointsL}}=_ElemL,
                {
                    {_GroupIdR, _GroupUsersR},
                    {_GroupUsersSumedR, GroupPointsR}}=_ElemR) ->
                GroupPointsL >= GroupPointsR
            end,
            OrderedGroupBySumGroupXPoints)),

    AllPathInfoList3 =
        [{InputSetGroupId, {undefined, undefined}}],
    SetCountGroupId = <<"gC">>,
    %% Create set_count object.
    {ok, {{SetCountGroupId, _}=SetCountGroup, _, _, _}} =
        lasp:declare(
            SetCountGroupId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList3, {InputSetGroupId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(ok, lasp:set_count(InputSetGroup, SetCountGroup)),

    AllPathInfoList5 =
        [
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupCountXDividerId = <<"gCXd">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupCountXDividerId, _}=AggResultGroupCountXDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupCountXDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList5, {SetCountGroupId, InputRegisterDividerId}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetCountGroup, InputRegisterDivider, AggResultGroupCountXDivider)),

    AllPathInfoList6 =
        [
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupCountXDividerRankId = <<"gCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupCountXDividerRankId, _}=AggGroupCountXDividerRank, _, _, _}} =
        lasp:declare(
            AggGroupCountXDividerRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList6, {AggResultGroupCountXDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupCountXDivider,
            fun({GroupCount, Divider}) ->
                {RankDivider, 100} =
                    lists:foldl(
                        fun(Border, {AccInRankDivider, AccInBorderSum}) ->
                            NewBorderSum = AccInBorderSum + Border,
                            Rank = GroupCount * NewBorderSum div 100,
                            {AccInRankDivider ++ [Rank], NewBorderSum}
                        end,
                        {[], 0},
                        Divider),
                RankDivider
            end,
            AggGroupCountXDividerRank)),

    AllPathInfoList7 =
        [
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupPointsOrderedXRankDividerId = <<"gXuFGOXgCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupPointsOrderedXRankDividerId, _}= AggResultGroupPointsOrderedXRankDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupPointsOrderedXRankDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList7, {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}]}),

    %% Apply product.
    ?assertMatch(
        ok,
        lasp:product(
            OrderedGroupBySumGroupXPoints,
            AggGroupCountXDividerRank,
            AggResultGroupPointsOrderedXRankDivider)),

    AllPathInfoList8 =
        [
            {
                AggResultGroupPointsOrderedXRankDividerId,
                {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}},
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupRankId = <<"gXuFGOXgCXdMM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupRankId, _}=AggGroupRank, _, _, _}} =
        lasp:declare(
            AggGroupRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList8, {AggResultGroupPointsOrderedXRankDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupPointsOrderedXRankDivider,
            fun({GroupPointsOrdered, RankDivider}) ->
                {_Index, GroupRanked, _CurRank, _CurRankDivider} =
                    lists:foldl(
                        fun({{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {
                                AccInIndex,
                                AccInGroupRanked,
                                AccInCurRank,
                                [CurDivider | RestDividers]=AccInRankDivider}) ->
                            {NewCurRank, NewRankDivider} =
                                case AccInIndex == CurDivider of
                                    true ->
                                        {AccInCurRank + 1, RestDividers};
                                    false ->
                                        {AccInCurRank, AccInRankDivider}
                                end,
                            {
                                AccInIndex + 1,
                                ordsets:add_element({GroupId, NewCurRank}, AccInGroupRanked),
                                NewCurRank,
                                NewRankDivider};
                        (
                            {{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {AccInIndex, AccInGroupRanked, AccInCurRank, []}) ->
                            {
                                AccInIndex + 1,
                                ordsets:add_element({GroupId, AccInCurRank}, AccInGroupRanked),
                                AccInCurRank,
                                []}
                        end,
                        {0, ordsets:new(), 1, RankDivider},
                        GroupPointsOrdered),
                GroupRanked
            end,
            AggGroupRank)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(AggGroupRank, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, AggGroupRankV1}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, AggGroupRankV1}),
                    AggGroupRankV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_2, 2000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(AggGroupRank, {strict, ReadValue1})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset2, ReadResult2}, ReadValue2} =
        receive
            {I2, {ok, {_, _, _, AggGroupRankV2}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, AggGroupRankV2}),
                    AggGroupRankV2}
        end,

    %% Bind again.
    ?assertMatch(
        {ok, _}, lasp:update(InputSetGroup, {add, {group_2, Group2}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I3 = third_read,
    spawn(
        fun() ->
            Me ! {I3, lasp:read(AggGroupRank, {strict, ReadValue2})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset3, _ReadResult3}, ReadValue3} =
        receive
            {I3, {ok, {_, _, _, AggGroupRankV3}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset2, AggGroupRankV3}),
                    AggGroupRankV3}
        end,

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I4 = forth_read,
    spawn(
        fun() ->
            Me ! {I4, lasp:read(AggGroupRank, {strict, ReadValue3})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{_Subset4, ReadResult4}, _ReadValue4} =
        receive
            {I4, {ok, {_, _, _, AggGroupRankV4}}} ->
                {
                    lasp_type:query_ext(?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset3, AggGroupRankV4}),
                    AggGroupRankV4}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetUserV1}} =
        lasp:read(InputSetUser, {strict, undefined}),
    {ok, {_, _, _, InputSetGroupV1}} =
        lasp:read(InputSetGroup, {strict, undefined}),
    {ok, {_, _, _, InputRegisterDividerV1}} =
        lasp:read(InputRegisterDivider, {strict, undefined}),

    {ok, {_, _, _, InterSetGroupXUserV1}} =
        lasp:read(InterSetGroupXUser, {strict, undefined}),

    {ok, {_, _, _, InterSetGroupXUserMatchV1}} =
        lasp:read(InterSetGroupXUserMatch, {strict, undefined}),

    {ok, {_, _, _, GroupBySumGroupXPointsV1}} =
        lasp:read(GroupBySumGroupXPoints, {strict, undefined}),

    {ok, {_, _, _, OrderedGroupBySumGroupXPointsV1}} =
        lasp:read(OrderedGroupBySumGroupXPoints, {strict, undefined}),

    {ok, {_, _, _, SetCountGroupV1}} =
        lasp:read(SetCountGroup, {strict, undefined}),

    {ok, {_, _, _, AggResultGroupCountXDividerV1}} =
        lasp:read(AggResultGroupCountXDivider, {strict, undefined}),

    {ok, {_, _, _, AggGroupCountXDividerRankV1}} =
        lasp:read(AggGroupCountXDividerRank, {strict, undefined}),

    {ok, {_, _, _, AggResultGroupPointsOrderedXRankDividerV1}} =
        lasp:read(AggResultGroupPointsOrderedXRankDivider, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([{user_1, 1000}, {user_2, 2000}]),
            sets:from_list([{group_1, Group1}, {group_2, Group2}]),
            sets:from_list([[50, 50]]),
            sets:from_list(
                [
                    {{group_1, Group1}, {user_1, 1000}},
                    {{group_1, Group1}, {user_2, 2000}},
                    {{group_2, Group2}, {user_1, 1000}},
                    {{group_2, Group2}, {user_2, 2000}}]),
            sets:from_list(
                [
                    {{group_1, Group1}, {user_1, 1000}},
                    {{group_1, Group1}, {user_2, 2000}},
                    {{group_2, Group2}, {user_2, 2000}}]),
            sets:from_list(
                [
                    {{group_1, Group1}, {Group1, 3000}},
                    {{group_2, Group2}, {Group2, 2000}}]),
            sets:from_list(
                [
                    [
                        {{group_1, Group1}, {Group1, 3000}},
                        {{group_2, Group2}, {Group2, 2000}}]]),
            sets:from_list([2]),
            sets:from_list([{2, [50, 50]}]),
            sets:from_list([[1, 2]]),
            sets:from_list(
                [
                    {
                        [
                            {{group_1, Group1}, {Group1, 3000}},
                            {{group_2, Group2}, {Group2, 2000}}],
                        [1, 2]}]),
            sets:from_list([ordsets:from_list([{group_1, 2}])]),
            sets:from_list([ordsets:from_list([{group_1, 2}])]),
            sets:from_list([ordsets:from_list([{group_1, 1}, {group_2, 2}])])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetUserV1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetGroupV1),
            lasp_type:query(?EXT_LWWREGISTER_INPUT_TYPE, InputRegisterDividerV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, InterSetGroupXUserV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, InterSetGroupXUserMatchV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, GroupBySumGroupXPointsV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, OrderedGroupBySumGroupXPointsV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, SetCountGroupV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggResultGroupCountXDividerV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggGroupCountXDividerRankV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggResultGroupPointsOrderedXRankDividerV1),
            ReadResult1,
            ReadResult2,
            ReadResult4}),

    ok.

%% @doc Group rank unit test with query_ext_consistent().
group_rank_query_ext_consistent_test(_Config) ->
    Group1 = ordsets:from_list([user_1, user_2]),
    Group2 = ordsets:from_list([user_2]),

    %% Create initial sets.
    InputSetAllPathInfoList = [],
    InputSetUserId = <<"u">>,
    {ok, {{InputSetUserId, _}=InputSetUser, _, _, _}} =
        lasp:declare(
            InputSetUserId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputSetGroupId = <<"g">>,
    {ok, {{InputSetGroupId, _}=InputSetGroup, _, _, _}} =
        lasp:declare(
            InputSetGroupId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputRegisterDividerId = <<"d">>,
    {ok, {{InputRegisterDividerId, _}=InputRegisterDivider, _, _, _}} =
        lasp:declare(
            InputRegisterDividerId,
            {?EXT_LWWREGISTER_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial sets and update.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_1, 1000}}, "a")),
    ?assertMatch(
        {ok, _}, lasp:update(InputSetGroup, {add, {group_1, Group1}}, "a")),
    ?assertMatch(
        {ok, _}, lasp:update(InputRegisterDivider, {write, [50, 50]}, "a")),

    %% Create an intermediate set.
    InterSetGroupXUserAllPathInfoList =
        [
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserId = <<"gXu">>,
    {ok, {{InterSetGroupXUserId, _}=InterSetGroupXUser, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserAllPathInfoList, {InputSetGroupId, InputSetUserId}]}),

    %% Apply product.
    ?assertMatch(
        ok, lasp:product(InputSetGroup, InputSetUser, InterSetGroupXUser)),

    %% Create an intermediate set.
    InterSetGroupXUserMatchAllPathInfoList =
        [
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserMatchId = <<"gXuF">>,
    {ok, {{InterSetGroupXUserMatchId, _}=InterSetGroupXUserMatch, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserMatchId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserMatchAllPathInfoList, {InterSetGroupXUserId, undefined}]}),

    %% Apply filter.
    ?assertMatch(
        ok,
        lasp:filter(
            InterSetGroupXUser,
            fun({{_GroupId, UserIdSet}, {UserId, _UserPoints}}) ->
                ordsets:is_element(UserId, UserIdSet)
            end,
            InterSetGroupXUserMatch)),

    AllPathInfoList0 =
        [
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    GroupBySumGroupXPointsId = <<"gXuFG">>,
    %% Create group_by_sum object.
    {ok, {{GroupBySumGroupXPointsId, _}=GroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            GroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InterSetGroupXUserMatchId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:group_by_sum(
            InterSetGroupXUserMatch,
            fun(undefined, {UserIdR, UserPointsR}) ->
                {
                    ordsets:add_element(UserIdR, ordsets:new()),
                    0 + UserPointsR};
                ({UserIdL, UserPointsL}, {UserIdR, UserPointsR}) ->
                    {
                        ordsets:add_element(UserIdR, UserIdL),
                        UserPointsL + UserPointsR}
            end,
            GroupBySumGroupXPoints)),

    AllPathInfoList1 =
        [
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    OrderedGroupBySumGroupXPointsId = <<"gXuFGO">>,
    %% Create order_by object.
    {ok, {{OrderedGroupBySumGroupXPointsId, _}=OrderedGroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            OrderedGroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList1, {GroupBySumGroupXPointsId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:order_by(
            GroupBySumGroupXPoints,
            fun({
                {_GroupIdL, _GroupUsersL},
                {_GroupUsersSumedL, GroupPointsL}}=_ElemL,
                {
                    {_GroupIdR, _GroupUsersR},
                    {_GroupUsersSumedR, GroupPointsR}}=_ElemR) ->
                GroupPointsL >= GroupPointsR
            end,
            OrderedGroupBySumGroupXPoints)),

    AllPathInfoList3 =
        [{InputSetGroupId, {undefined, undefined}}],
    SetCountGroupId = <<"gC">>,
    %% Create set_count object.
    {ok, {{SetCountGroupId, _}=SetCountGroup, _, _, _}} =
        lasp:declare(
            SetCountGroupId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList3, {InputSetGroupId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(ok, lasp:set_count(InputSetGroup, SetCountGroup)),

    AllPathInfoList5 =
        [
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupCountXDividerId = <<"gCXd">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupCountXDividerId, _}=AggResultGroupCountXDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupCountXDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList5, {SetCountGroupId, InputRegisterDividerId}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetCountGroup, InputRegisterDivider, AggResultGroupCountXDivider)),

    AllPathInfoList6 =
        [
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupCountXDividerRankId = <<"gCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupCountXDividerRankId, _}=AggGroupCountXDividerRank, _, _, _}} =
        lasp:declare(
            AggGroupCountXDividerRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList6, {AggResultGroupCountXDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupCountXDivider,
            fun({GroupCount, Divider}) ->
                {RankDivider, 100} =
                    lists:foldl(
                        fun(Border, {AccInRankDivider, AccInBorderSum}) ->
                            NewBorderSum = AccInBorderSum + Border,
                            Rank = GroupCount * NewBorderSum div 100,
                            {AccInRankDivider ++ [Rank], NewBorderSum}
                        end,
                        {[], 0},
                        Divider),
                RankDivider
            end,
            AggGroupCountXDividerRank)),

    AllPathInfoList7 =
        [
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupPointsOrderedXRankDividerId = <<"gXuFGOXgCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupPointsOrderedXRankDividerId, _}= AggResultGroupPointsOrderedXRankDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupPointsOrderedXRankDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList7, {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}]}),

    %% Apply product.
    ?assertMatch(
        ok,
        lasp:product(
            OrderedGroupBySumGroupXPoints,
            AggGroupCountXDividerRank,
            AggResultGroupPointsOrderedXRankDivider)),

    AllPathInfoList8 =
        [
            {
                AggResultGroupPointsOrderedXRankDividerId,
                {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}},
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupRankId = <<"gXuFGOXgCXdMM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupRankId, _}=AggGroupRank, _, _, _}} =
        lasp:declare(
            AggGroupRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList8, {AggResultGroupPointsOrderedXRankDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupPointsOrderedXRankDivider,
            fun({GroupPointsOrdered, RankDivider}) ->
                {_Index, GroupRanked, _CurRank, _CurRankDivider} =
                    lists:foldl(
                        fun({{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {
                                AccInIndex,
                                AccInGroupRanked,
                                AccInCurRank,
                                [CurDivider | RestDividers]=AccInRankDivider}) ->
                            {NewCurRank, NewRankDivider} =
                                case AccInIndex == CurDivider of
                                    true ->
                                        {AccInCurRank + 1, RestDividers};
                                    false ->
                                        {AccInCurRank, AccInRankDivider}
                                end,
                            {
                                AccInIndex + 1,
                                ordsets:add_element({GroupId, NewCurRank}, AccInGroupRanked),
                                NewCurRank,
                                NewRankDivider};
                            (
                                {{GroupId, _UserSet}, {_SumedUsers, _Points}},
                                {AccInIndex, AccInGroupRanked, AccInCurRank, []}) ->
                                {
                                    AccInIndex + 1,
                                    ordsets:add_element({GroupId, AccInCurRank}, AccInGroupRanked),
                                    AccInCurRank,
                                    []}
                        end,
                        {0, ordsets:new(), 1, RankDivider},
                        GroupPointsOrdered),
                GroupRanked
            end,
            AggGroupRank)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(AggGroupRank, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    InitCDS = ordsets:new(),
    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset1, CDS1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, AggGroupRankV1}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, InitCDS, AggGroupRankV1}),
                    AggGroupRankV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_2, 2000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(AggGroupRank, {strict, ReadValue1})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset2, CDS2, ReadResult2}, ReadValue2} =
        receive
            {I2, {ok, {_, _, _, AggGroupRankV2}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, CDS1, AggGroupRankV2}),
                    AggGroupRankV2}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetGroup, {add, {group_2, Group2}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I3 = third_read,
    spawn(
        fun() ->
            Me ! {I3, lasp:read(AggGroupRank, {strict, ReadValue2})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset3, CDS3, ReadResult3}, ReadValue3} =
        receive
            {I3, {ok, {_, _, _, AggGroupRankV3}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset2, CDS2, AggGroupRankV3}),
                    AggGroupRankV3}
        end,

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I4 = forth_read,
    spawn(
        fun() ->
            Me ! {I4, lasp:read(AggGroupRank, {strict, ReadValue3})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{_Subset4, _CDS4, ReadResult4}, _ReadValue4} =
        receive
            {I4, {ok, {_, _, _, AggGroupRankV4}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset3, CDS3, AggGroupRankV4}),
                    AggGroupRankV4}
        end,

    %% Read resulting value.
    {ok, {_, _, _, InputSetUserV1}} =
        lasp:read(InputSetUser, {strict, undefined}),
    {ok, {_, _, _, InputSetGroupV1}} =
        lasp:read(InputSetGroup, {strict, undefined}),
    {ok, {_, _, _, InputRegisterDividerV1}} =
        lasp:read(InputRegisterDivider, {strict, undefined}),

    {ok, {_, _, _, InterSetGroupXUserV1}} =
        lasp:read(InterSetGroupXUser, {strict, undefined}),

    {ok, {_, _, _, InterSetGroupXUserMatchV1}} =
        lasp:read(InterSetGroupXUserMatch, {strict, undefined}),

    {ok, {_, _, _, GroupBySumGroupXPointsV1}} =
        lasp:read(GroupBySumGroupXPoints, {strict, undefined}),

    {ok, {_, _, _, OrderedGroupBySumGroupXPointsV1}} =
        lasp:read(OrderedGroupBySumGroupXPoints, {strict, undefined}),

    {ok, {_, _, _, SetCountGroupV1}} =
        lasp:read(SetCountGroup, {strict, undefined}),

    {ok, {_, _, _, AggResultGroupCountXDividerV1}} =
        lasp:read(AggResultGroupCountXDivider, {strict, undefined}),

    {ok, {_, _, _, AggGroupCountXDividerRankV1}} =
        lasp:read(AggGroupCountXDividerRank, {strict, undefined}),

    {ok, {_, _, _, AggResultGroupPointsOrderedXRankDividerV1}} =
        lasp:read(AggResultGroupPointsOrderedXRankDivider, {strict, undefined}),

    ?assertEqual(
        {
            ok,
            sets:from_list([{user_1, 1000}, {user_2, 2000}]),
            sets:from_list([{group_1, Group1}, {group_2, Group2}]),
            sets:from_list([[50, 50]]),
            sets:from_list(
                [
                    {{group_1, Group1}, {user_1, 1000}},
                    {{group_1, Group1}, {user_2, 2000}},
                    {{group_2, Group2}, {user_1, 1000}},
                    {{group_2, Group2}, {user_2, 2000}}]),
            sets:from_list(
                [
                    {{group_1, Group1}, {user_1, 1000}},
                    {{group_1, Group1}, {user_2, 2000}},
                    {{group_2, Group2}, {user_2, 2000}}]),
            sets:from_list(
                [
                    {{group_1, Group1}, {Group1, 3000}},
                    {{group_2, Group2}, {Group2, 2000}}]),
            sets:from_list(
                [
                    [
                        {{group_1, Group1}, {Group1, 3000}},
                        {{group_2, Group2}, {Group2, 2000}}]]),
            sets:from_list([2]),
            sets:from_list([{2, [50, 50]}]),
            sets:from_list([[1, 2]]),
            sets:from_list(
                [
                    {
                        [
                            {{group_1, Group1}, {Group1, 3000}},
                            {{group_2, Group2}, {Group2, 2000}}],
                        [1, 2]}])},
        {
            ok,
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetUserV1),
            lasp_type:query(?EXT_AWORSET_INPUT_TYPE, InputSetGroupV1),
            lasp_type:query(?EXT_LWWREGISTER_INPUT_TYPE, InputRegisterDividerV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, InterSetGroupXUserV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, InterSetGroupXUserMatchV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, GroupBySumGroupXPointsV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, OrderedGroupBySumGroupXPointsV1),
            lasp_type:query(?EXT_AWORSET_AGGRESULT_TYPE, SetCountGroupV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggResultGroupCountXDividerV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggGroupCountXDividerRankV1),
            lasp_type:query(?EXT_AWORSET_INTERMEDIATE_TYPE, AggResultGroupPointsOrderedXRankDividerV1)}),

    ?assert(ReadResult1 == sets:from_list([ordsets:from_list([{group_1, 2}])])),
    ?assert(ReadResult2 == sets:from_list([ordsets:from_list([{group_1, 2}])])),
    ?assert(
        ReadResult3 == sets:new() orelse
            ReadResult3 == sets:from_list([ordsets:from_list([{group_1, 2}])])),
    ?assert(ReadResult4 == sets:from_list([ordsets:from_list([{group_1, 1}, {group_2, 2}])])),

    ok.

%% @doc Group rank unit test with query_ext_consistent().
group_rank_query_ext_consistent_complex_test(_Config) ->
    Group1 = ordsets:from_list([user_1, user_2]),
    Group2 = ordsets:from_list([user_3]),
    Group3 = ordsets:from_list([user_2]),
    Group4 = ordsets:from_list([user_1, user_3]),

    %% Create initial sets.
    InputSetAllPathInfoList = [],
    InputSetUserId = <<"u">>,
    {ok, {{InputSetUserId, _}=InputSetUser, _, _, _}} =
        lasp:declare(
            InputSetUserId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputSetGroupId = <<"g">>,
    {ok, {{InputSetGroupId, _}=InputSetGroup, _, _, _}} =
        lasp:declare(
            InputSetGroupId,
            {?EXT_AWORSET_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),
    InputRegisterDividerId = <<"d">>,
    {ok, {{InputRegisterDividerId, _}=InputRegisterDivider, _, _, _}} =
        lasp:declare(
            InputRegisterDividerId,
            {?EXT_LWWREGISTER_INPUT_TYPE, [InputSetAllPathInfoList, {undefined, undefined}]}),

    %% Add elements to initial sets and update.
    ?assertMatch(
        {ok, _}, lasp:update(InputRegisterDivider, {write, [30, 30, 40]}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_1, 1000}}, "a")),
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_2, 2000}}, "a")),
    ?assertMatch(
        {ok, _}, lasp:update(InputSetGroup, {add, {group_1, Group1}}, "a")),

    %% Create an intermediate set.
    InterSetGroupXUserAllPathInfoList =
        [
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserId = <<"gXu">>,
    {ok, {{InterSetGroupXUserId, _}=InterSetGroupXUser, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserAllPathInfoList, {InputSetGroupId, InputSetUserId}]}),

    %% Apply product.
    ?assertMatch(
        ok, lasp:product(InputSetGroup, InputSetUser, InterSetGroupXUser)),

    %% Create an intermediate set.
    InterSetGroupXUserMatchAllPathInfoList =
        [
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    InterSetGroupXUserMatchId = <<"gXuF">>,
    {ok, {{InterSetGroupXUserMatchId, _}=InterSetGroupXUserMatch, _, _, _}} =
        lasp:declare(
            InterSetGroupXUserMatchId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [InterSetGroupXUserMatchAllPathInfoList, {InterSetGroupXUserId, undefined}]}),

    %% Apply filter.
    ?assertMatch(
        ok,
        lasp:filter(
            InterSetGroupXUser,
            fun({{_GroupId, UserIdSet}, {UserId, _UserPoints}}) ->
                ordsets:is_element(UserId, UserIdSet)
            end,
            InterSetGroupXUserMatch)),

    AllPathInfoList0 =
        [
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    GroupBySumGroupXPointsId = <<"gXuFG">>,
    %% Create group_by_sum object.
    {ok, {{GroupBySumGroupXPointsId, _}=GroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            GroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList0, {InterSetGroupXUserMatchId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:group_by_sum(
            InterSetGroupXUserMatch,
            fun(undefined, {UserIdR, UserPointsR}) ->
                {
                    ordsets:add_element(UserIdR, ordsets:new()),
                    0 + UserPointsR};
                ({UserIdL, UserPointsL}, {UserIdR, UserPointsR}) ->
                    {
                        ordsets:add_element(UserIdR, UserIdL),
                        UserPointsL + UserPointsR}
            end,
            GroupBySumGroupXPoints)),

    AllPathInfoList1 =
        [
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}}],
    OrderedGroupBySumGroupXPointsId = <<"gXuFGO">>,
    %% Create order_by object.
    {ok, {{OrderedGroupBySumGroupXPointsId, _}=OrderedGroupBySumGroupXPoints, _, _, _}} =
        lasp:declare(
            OrderedGroupBySumGroupXPointsId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList1, {GroupBySumGroupXPointsId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(
        ok,
        lasp:order_by(
            GroupBySumGroupXPoints,
            fun({
                {_GroupIdL, _GroupUsersL},
                {_GroupUsersSumedL, GroupPointsL}}=_ElemL,
                {
                    {_GroupIdR, _GroupUsersR},
                    {_GroupUsersSumedR, GroupPointsR}}=_ElemR) ->
                GroupPointsL >= GroupPointsR
            end,
            OrderedGroupBySumGroupXPoints)),

    AllPathInfoList3 =
        [{InputSetGroupId, {undefined, undefined}}],
    SetCountGroupId = <<"gC">>,
    %% Create set_count object.
    {ok, {{SetCountGroupId, _}=SetCountGroup, _, _, _}} =
        lasp:declare(
            SetCountGroupId,
            {?EXT_AWORSET_AGGRESULT_TYPE, [AllPathInfoList3, {InputSetGroupId, undefined}]}),

    %% Apply set_count.
    ?assertMatch(ok, lasp:set_count(InputSetGroup, SetCountGroup)),

    AllPathInfoList5 =
        [
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupCountXDividerId = <<"gCXd">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupCountXDividerId, _}=AggResultGroupCountXDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupCountXDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList5, {SetCountGroupId, InputRegisterDividerId}]}),

    %% Apply product.
    ?assertMatch(ok, lasp:product(SetCountGroup, InputRegisterDivider, AggResultGroupCountXDivider)),

    AllPathInfoList6 =
        [
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupCountXDividerRankId = <<"gCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupCountXDividerRankId, _}=AggGroupCountXDividerRank, _, _, _}} =
        lasp:declare(
            AggGroupCountXDividerRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList6, {AggResultGroupCountXDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupCountXDivider,
            fun({GroupCount, Divider}) ->
                {RankDivider, 100} =
                    lists:foldl(
                        fun(Border, {AccInRankDivider, AccInBorderSum}) ->
                            NewBorderSum = AccInBorderSum + Border,
                            Rank = GroupCount * NewBorderSum div 100,
                            {AccInRankDivider ++ [Rank], NewBorderSum}
                        end,
                        {[], 0},
                        Divider),
                RankDivider
            end,
            AggGroupCountXDividerRank)),

    AllPathInfoList7 =
        [
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggResultGroupPointsOrderedXRankDividerId = <<"gXuFGOXgCXdM">>,
    %% Create agg_result_general object.
    {ok, {{AggResultGroupPointsOrderedXRankDividerId, _}= AggResultGroupPointsOrderedXRankDivider, _, _, _}} =
        lasp:declare(
            AggResultGroupPointsOrderedXRankDividerId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList7, {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}]}),

    %% Apply product.
    ?assertMatch(
        ok,
        lasp:product(
            OrderedGroupBySumGroupXPoints,
            AggGroupCountXDividerRank,
            AggResultGroupPointsOrderedXRankDivider)),

    AllPathInfoList8 =
        [
            {
                AggResultGroupPointsOrderedXRankDividerId,
                {OrderedGroupBySumGroupXPointsId, AggGroupCountXDividerRankId}},
            {OrderedGroupBySumGroupXPointsId, {GroupBySumGroupXPointsId, undefined}},
            {GroupBySumGroupXPointsId, {InterSetGroupXUserMatchId, undefined}},
            {InterSetGroupXUserMatchId, {InterSetGroupXUserId, undefined}},
            {InterSetGroupXUserId, {InputSetGroupId, InputSetUserId}},
            {InputSetGroupId, {undefined, undefined}},
            {InputSetUserId, {undefined, undefined}},
            {AggGroupCountXDividerRankId, {AggResultGroupCountXDividerId, undefined}},
            {AggResultGroupCountXDividerId, {SetCountGroupId, InputRegisterDividerId}},
            {SetCountGroupId, {InputSetGroupId, undefined}},
            {InputSetGroupId, {undefined, undefined}},
            {InputRegisterDividerId, {undefined, undefined}}],
    AggGroupRankId = <<"gXuFGOXgCXdMM">>,
    %% Create agg_result_general object.
    {ok, {{AggGroupRankId, _}=AggGroupRank, _, _, _}} =
        lasp:declare(
            AggGroupRankId,
            {
                ?EXT_AWORSET_INTERMEDIATE_TYPE,
                [AllPathInfoList8, {AggResultGroupPointsOrderedXRankDividerId, undefined}]}),

    %% Apply map.
    ?assertMatch(
        ok, lasp:map(
            AggResultGroupPointsOrderedXRankDivider,
            fun({GroupPointsOrdered, RankDivider}) ->
                {_Index, GroupRanked, _CurRank, _CurRankDivider} =
                    lists:foldl(
                        fun({{GroupId, _UserSet}, {_SumedUsers, _Points}},
                            {
                                AccInIndex,
                                AccInGroupRanked,
                                AccInCurRank,
                                [CurDivider | RestDividers]=AccInRankDivider}) ->
                            {NewCurRank, NewRankDivider} =
                                case AccInIndex == CurDivider of
                                    true ->
                                        {AccInCurRank + 1, RestDividers};
                                    false ->
                                        {AccInCurRank, AccInRankDivider}
                                end,
                            {
                                AccInIndex + 1,
                                ordsets:add_element({GroupId, NewCurRank}, AccInGroupRanked),
                                NewCurRank,
                                NewRankDivider};
                            (
                                {{GroupId, _UserSet}, {_SumedUsers, _Points}},
                                {AccInIndex, AccInGroupRanked, AccInCurRank, []}) ->
                                {
                                    AccInIndex + 1,
                                    ordsets:add_element({GroupId, AccInCurRank}, AccInGroupRanked),
                                    AccInCurRank,
                                    []}
                        end,
                        {0, ordsets:new(), 1, RankDivider},
                        GroupPointsOrdered),
                GroupRanked
            end,
            AggGroupRank)),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I1 = first_read,
    spawn(
        fun() ->
            Me ! {I1, lasp:read(AggGroupRank, {strict, undefined})}
        end),

    InitSubset = ordsets:new(),
    InitCDS = ordsets:new(),
    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset1, CDS1, ReadResult1}, ReadValue1} =
        receive
            {I1, {ok, {_, _, _, AggGroupRankV1}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {InitSubset, InitCDS, AggGroupRankV1}),
                    AggGroupRankV1}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_3, 8000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I2 = second_read,
    spawn(
        fun() ->
            Me ! {I2, lasp:read(AggGroupRank, {strict, ReadValue1})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset2, CDS2, ReadResult2}, ReadValue2} =
        receive
            {I2, {ok, {_, _, _, AggGroupRankV2}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset1, CDS1, AggGroupRankV2}),
                    AggGroupRankV2}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetGroup, {add, {group_2, Group2}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I3 = third_read,
    spawn(
        fun() ->
            Me ! {I3, lasp:read(AggGroupRank, {strict, ReadValue2})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset3, CDS3, ReadResult3}, ReadValue3} =
        receive
            {I3, {ok, {_, _, _, AggGroupRankV3}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset2, CDS2, AggGroupRankV3}),
                    AggGroupRankV3}
        end,

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I4 = forth_read,
    spawn(
        fun() ->
            Me ! {I4, lasp:read(AggGroupRank, {strict, ReadValue3})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset4, CDS4, ReadResult4}, ReadValue4} =
        receive
            {I4, {ok, {_, _, _, AggGroupRankV4}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset3, CDS3, AggGroupRankV4}),
                    AggGroupRankV4}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_2, 9000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I5 = fifth_read,
    spawn(
        fun() ->
            Me ! {I5, lasp:read(AggGroupRank, {strict, ReadValue4})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset5, CDS5, ReadResult5}, ReadValue5} =
        receive
            {I5, {ok, {_, _, _, AggGroupRankV5}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset4, CDS4, AggGroupRankV5}),
                    AggGroupRankV5}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetGroup, {add, {group_3, Group3}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I6 = sixth_read,
    spawn(
        fun() ->
            Me ! {I6, lasp:read(AggGroupRank, {strict, ReadValue5})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset6, CDS6, ReadResult6}, ReadValue6} =
        receive
            {I6, {ok, {_, _, _, AggGroupRankV6}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset5, CDS5, AggGroupRankV6}),
                    AggGroupRankV6}
        end,

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I7 = seventh_read,
    spawn(
        fun() ->
            Me ! {I7, lasp:read(AggGroupRank, {strict, ReadValue6})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset7, CDS7, ReadResult7}, ReadValue7} =
        receive
            {I7, {ok, {_, _, _, AggGroupRankV7}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset6, CDS6, AggGroupRankV7}),
                    AggGroupRankV7}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_2, 5000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I8 = eighth_read,
    spawn(
        fun() ->
            Me ! {I8, lasp:read(AggGroupRank, {strict, ReadValue7})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset8, CDS8, ReadResult8}, ReadValue8} =
        receive
            {I8, {ok, {_, _, _, AggGroupRankV8}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset7, CDS7, AggGroupRankV8}),
                    AggGroupRankV8}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_3, 3000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I9 = ninth_read,
    spawn(
        fun() ->
            Me ! {I9, lasp:read(AggGroupRank, {strict, ReadValue8})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset9, CDS9, ReadResult9}, ReadValue9} =
        receive
            {I9, {ok, {_, _, _, AggGroupRankV9}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset8, CDS8, AggGroupRankV9}),
                    AggGroupRankV9}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetUser, {add, {user_3, 1000}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I10 = tenth_read,
    spawn(
        fun() ->
            Me ! {I10, lasp:read(AggGroupRank, {strict, ReadValue9})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset10, CDS10, ReadResult10}, ReadValue10} =
        receive
            {I10, {ok, {_, _, _, AggGroupRankV10}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset9, CDS9, AggGroupRankV10}),
                    AggGroupRankV10}
        end,

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(InputSetGroup, {add, {group_4, Group4}}, "a")),

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I11 = eleventh_read,
    spawn(
        fun() ->
            Me ! {I11, lasp:read(AggGroupRank, {strict, ReadValue10})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{Subset11, CDS11, ReadResult11}, ReadValue11} =
        receive
            {I11, {ok, {_, _, _, AggGroupRankV11}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset10, CDS10, AggGroupRankV11}),
                    AggGroupRankV11}
        end,

    %% Spawn fun which should block until lattice is strict inflation of
    %% undefined (the bottom element).
    I12 = twelveth_read,
    spawn(
        fun() ->
            Me ! {I12, lasp:read(AggGroupRank, {strict, ReadValue11})}
        end),

    %% Ensure we receive [{{1, 2}, {2, 3}}].
    {{_Subset12, _CDS12, ReadResult12}, _ReadValue12} =
        receive
            {I12, {ok, {_, _, _, AggGroupRankV12}}} ->
                {
                    lasp_type:query_ext_consistent(
                        ?EXT_AWORSET_INTERMEDIATE_TYPE, {Subset11, CDS11, AggGroupRankV12}),
                    AggGroupRankV12}
        end,

    ?assertEqual(sets:from_list([ordsets:from_list([{group_1, 2}])]), ReadResult1),
    ?assertEqual(sets:from_list([ordsets:from_list([{group_1, 2}])]), ReadResult2),
    ?assert(ReadResult3 == sets:new() orelse ReadResult3 == ReadResult2),
    ?assertEqual(sets:from_list([ordsets:from_list([{group_1, 3}, {group_2, 2}])]), ReadResult4),
    ?assertEqual(sets:from_list([ordsets:from_list([{group_1, 2}, {group_2, 3}])]), ReadResult5),
    ?assert(ReadResult6 == sets:new() orelse ReadResult6 == ReadResult5),
    ?assertEqual(
        sets:from_list([ordsets:from_list([{group_1, 2}, {group_2, 3}, {group_3, 3}])]), ReadResult7),
    ?assertEqual(
        sets:from_list([ordsets:from_list([{group_1, 2}, {group_2, 3}, {group_3, 3}])]), ReadResult8),
    ?assertEqual(
        sets:from_list([ordsets:from_list([{group_1, 2}, {group_2, 3}, {group_3, 3}])]), ReadResult9),
    ?assertEqual(
        sets:from_list([ordsets:from_list([{group_1, 2}, {group_2, 3}, {group_3, 3}])]), ReadResult10),
    ?assert(ReadResult11 == sets:new() orelse ReadResult11 == ReadResult10),
    ?assertEqual(
        sets:from_list([ordsets:from_list([{group_1, 1}, {group_2, 3}, {group_3, 2}, {group_4, 3}])]),
        ReadResult12),

    ok.
