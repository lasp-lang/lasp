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

-module(lasp_SUITE).
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

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(PEER_SERVICE, partisan_hyparview_peer_service_manager).

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
     stream_test,
     query_test,
     ivar_test,
     orset_test,
     dynamic_ivar_test,
     monotonic_read_test,
     map_test,
     filter_test,
     union_test,
     product_test,
     intersection_test,
     membership_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

-define(SET, orset).
-define(COUNTER, pncounter).

-define(ID, <<"myidentifier">>).

%% @doc Increment counter and test stream behaviour.
stream_test(_Config) ->
    %% Declare a variable.
    {ok, {C1, _, _, _}} = lasp:declare(?COUNTER),

    %% Stream results.
    Self = self(),
    Function = fun(V) ->
                       Self ! {ok, V}
               end,
    ?assertMatch(ok, lasp:stream(C1, Function)),

    %% Increment.
    ?assertMatch({ok, _}, lasp:update(C1, increment, a)),

    %% Wait for message.
    receive
        {ok, 1} ->
            ok
    after
        300 ->
            ct:fail(missing_first_message)
    end,

    %% Increment again.
    ?assertMatch({ok, _}, lasp:update(C1, increment, b)),

    %% Wait for message.
    receive
        {ok, 2} ->
            ok
    after
        300 ->
            ct:fail(missing_second_message)
    end.

%% @doc Test query functionality.
query_test(_Config) ->
    %% Declare a variable.
    {ok, {I1, _, _, _}} = lasp:declare(ivar),

    %% Change it's value.
    ?assertMatch({ok, _}, lasp:update(I1, {set, 2}, a)),

    %% Threshold read just to create a synchronization point for the
    %% value to change.
    {ok, _} = lasp:read(I1, {strict, undefined}),

    %% Query it.
    ?assertMatch({ok, 2}, lasp:query(I1)),

    ok.

%% @doc Single-assignment variable test.
ivar_test(_Config) ->
    %% Single-assignment variables.
    {ok, {I1, _, _, _}} = lasp:declare(ivar),
    {ok, {I2, _, _, _}} = lasp:declare(ivar),
    {ok, {I3, _, _, _}} = lasp:declare(ivar),

    V1 = 1,

    %% Attempt pre, and post- dataflow variable bind operations.
    ?assertMatch(ok, lasp:bind_to(I2, I1)),
    ?assertMatch({ok, _}, lasp:update(I1, {set, V1}, a)),
    ?assertMatch(ok, lasp:bind_to(I3, I1)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I3, {strict, undefined})),
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I2, {strict, undefined})),
    ?assertMatch({ok, {_, _, _, {_, V1}}}, lasp:read(I1, {strict, undefined})),

    ok.

%% @doc Map operation test.
map_test(_Config) ->
    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply map.
    ?assertMatch(ok, lasp:map(S1, fun(X) -> X * 2 end, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [4,5,6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S1V4}} = lasp:read(S1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),

    ?assertEqual({ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6,8,10,12])},
                 {ok, lasp_type:query(?SET, S1V4), lasp_type:query(?SET, S2V1)}),

    ok.

%% @doc Filter operation test.
filter_test(_Config) ->
    %% Create initial set.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),

    %% Add elements to initial set and update.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),

    %% Create second set.
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Apply filter.
    ?assertMatch(ok, lasp:filter(S1, fun(X) -> X rem 2 == 0 end, S2)),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [4,5,6]}, a)),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S1V4}} = lasp:read(S1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),

    ?assertEqual({ok, sets:from_list([1,2,3,4,5,6]), sets:from_list([2,4,6])},
                 {ok, lasp_type:query(?SET, S1V4), lasp_type:query(?SET, S2V1)}),

    ok.

%% @doc Union operation test.
union_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,c]}, a)),

    %% Apply union.
    ?assertMatch(ok, lasp:union(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read union.
    {ok, {_, _, _, Union0}} = lasp:read(S3, undefined),

    %% Read union value.
    Union = lasp_type:query(?SET, Union0),

    ?assertEqual({ok, sets:from_list([1,2,3,a,b,c])}, {ok, Union}),

    ok.

%% @doc Intersection test.
intersection_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,3]}, a)),

    %% Apply intersection.
    ?assertMatch(ok, lasp:intersection(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read intersection.
    {ok, {_, _, _, Intersection0}} = lasp:read(S3, undefined),

    %% Read intersection value.
    Intersection = lasp_type:query(?SET, Intersection0),

    ?assertEqual({ok, sets:from_list([3])}, {ok, Intersection}),

    ok.

%% @doc Cartesian product test.
product_test(_Config) ->
    %% Create initial sets.
    {ok, {S1, _, _, _}} = lasp:declare(?SET),
    {ok, {S2, _, _, _}} = lasp:declare(?SET),

    %% Create output set.
    {ok, {S3, _, _, _}} = lasp:declare(?SET),

    %% Populate initial sets.
    ?assertMatch({ok, _}, lasp:update(S1, {add_all, [1,2,3]}, a)),
    ?assertMatch({ok, _}, lasp:update(S2, {add_all, [a,b,3]}, a)),

    %% Apply product.
    ?assertMatch(ok, lasp:product(S1, S2, S3)),

    %% Sleep.
    timer:sleep(400),

    %% Read product.
    {ok, {_, _, _, Product0}} = lasp:read(S3, undefined),

    %% Read product value.
    Product = lasp_type:query(?SET, Product0),

    ?assertEqual({ok,sets:from_list([{1,3},{1,a},{1,b},{2,3},{2,a},{2,b},{3,3},{3,a},{3,b}])}, {ok, Product}),

    ok.

%% @doc Monotonic read.
monotonic_read_test(_Config) ->
    %% Create new set-based CRDT.
    {ok, {SetId, _, _, _}} = lasp:declare(?SET),

    %% Determine my pid.
    Me = self(),

    %% Perform 4 binds, each an inflation.
    ?assertMatch({ok, _},
                 lasp:update(SetId, {add_all, [1]}, actor)),

    {ok, {_, _, _, V0}} = lasp:update(SetId, {add_all, [2]}, actor),

    ?assertMatch({ok, {_, _, _, _}},
                 lasp:update(SetId, {add_all, [3]}, actor)),

    %% Spawn fun which should block until lattice is strict inflation of
    %% V0.
    I1 = first_read,
    spawn(fun() -> Me ! {I1, lasp:read(SetId, {strict, V0})} end),

    %% Ensure we receive [1, 2, 3].
    Set1 = receive
        {I1, {ok, {_, _, _, X}}} ->
            lasp_type:query(?SET, X)
    end,

    %% Perform more inflations.
    {ok, {_, _, _, V1}} = lasp:update(SetId, {add_all, [4]}, actor),

    ?assertMatch({ok, _}, lasp:update(SetId, {add_all, [5]}, actor)),

    %% Spawn fun which should block until lattice is a strict inflation
    %% of V1.
    I2 = second_read,
    spawn(fun() -> Me ! {I2, lasp:read(SetId, {strict, V1})} end),

    %% Ensure we receive [1, 2, 3, 4].
    Set2 = receive
        {I2, {ok, {_, _, _, Y}}} ->
            lasp_type:query(?SET, Y)
    end,

    ?assertEqual({sets:from_list([1,2,3]), sets:from_list([1,2,3,4,5])},
                 {Set1, Set2}),

    ok.

%% @doc Dynamic variable test.
dynamic_ivar_test(Config) ->
    [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),

    %% Setup a dynamic variable.
    {ok, {Id, _, _, Value}} = rpc:call(Node1, lasp, declare_dynamic,
                                       [?ID, ivar]),

    %% Now, the following action should be idempotent.
    {ok, {Id, _, _, Value}} = rpc:call(Node2, lasp, declare_dynamic,
                                       [?ID, ivar]),

    %% Bind node 1's name to the value on node 1: this should not
    %% trigger a broadcast message because the variable is dynamic.
    ?assertMatch({ok, {Id, _, _, {_, Node1}}},
                 rpc:call(Node1, lasp, bind, [{?ID, ivar}, {state_ivar, Node1}])),

    %% Bind node 2's name to the value on node 2: this should not
    %% trigger a broadcast message because the variable is dynamic.
    ?assertMatch({ok, {Id, _, _, {_, Node2}}},
                 rpc:call(Node2, lasp, bind, [{?ID, ivar}, {state_ivar, Node2}])),

    %% Verify variable has the correct value.
    {ok, Node1} = rpc:call(Node1, lasp, query, [{?ID, ivar}]),

    %% Verify variable has the correct value.
    {ok, Node2} = rpc:call(Node2, lasp, query, [{?ID, ivar}]),

    ok.

%% @doc Test of the orset.
orset_test(_Config) ->
    {ok, {L1, _, _, _}} = lasp:declare(?SET),
    {ok, {L2, _, _, _}} = lasp:declare(?SET),
    {ok, {L3, _, _, _}} = lasp:declare(?SET),

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

    spawn_link(fun() ->
                  {ok, _} = lasp:wait_needed(L1, {strict, S1}),
                  Self ! threshold_met
               end),

    ?assertMatch({ok, _}, lasp:bind(L1, S2)),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, _, S2L3}} = lasp:read(L3, {strict, undefined}),
    ?assertEqual(S2L3, lasp_type:merge(?SET, S2, S2L3)),
    {ok, {_, _, _, S2L2}} = lasp:read(L2, {strict, undefined}),
    ?assertEqual(S2L2, lasp_type:merge(?SET, S2, S2L2)),
    {ok, {_, _, _, S2L1}} = lasp:read(L1, {strict, undefined}),
    ?assertEqual(S2L1, lasp_type:merge(?SET, S2, S2L1)),

    %% Read at the S2 threshold level.
    {ok, {_, _, _, _}} = lasp:read(L1, S2),

    %% Wait for wait_needed to unblock.
    receive
        threshold_met ->
            ok
    end,

    {ok, {L5, _, _, _}} = lasp:declare(?SET),
    {ok, {L6, _, _, _}} = lasp:declare(?SET),

    spawn_link(fun() ->
                {ok, _} = lasp:read_any([{L5, {strict, undefined}}, {L6, {strict, undefined}}]),
                Self ! read_any
        end),

    {ok, _} = lasp:update(L5, {add, 1}, a),

    receive
        read_any ->
            ok
    end,

    ok.

%% @doc Membership test.
membership_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("Nodes: ~p", [Nodes]),

    lager:info("Waiting for cluster to stabilize."),
    timer:sleep(10000),

    lists:foreach(fun(Node) ->
                        {ok, {state, Myself, Active, Passive, _, _, _}} = rpc:call(Node, ?PEER_SERVICE, state, []),
                        lager:info("~p; active: ~p", [Myself, sets:to_list(Active)]),
                        lager:info("~p; passive: ~p", [Myself, sets:to_list(Passive)])
                  end, Nodes),

    ok.
