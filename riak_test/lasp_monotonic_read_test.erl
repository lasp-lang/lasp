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

-module(lasp_monotonic_read_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0]).

-define(SET, lasp_orset).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = lasp_test_helpers:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    ?assertMatch({[1,2,3], [1,2,3,4,5]},
                 rpc:call(Node, ?MODULE, test, [])),

    pass.

-endif.

test() ->
    %% Create new set-based CRDT.
    {ok, {SetId, _, _, _}} = lasp:declare(?SET),

    %% Determine my pid.
    Me = self(),

    %% Perform 4 binds, each an inflation.
    {ok, _} = lasp:update(SetId, {add_all, [1]}, actor),
    {ok, {_, _, _, V0}} = lasp:update(SetId, {add_all, [2]}, actor),
    {ok, {_, _, _, _}} = lasp:update(SetId, {add_all, [3]}, actor),

    %% Spawn fun which should block until lattice is strict inflation of
    %% V0.
    I1 = first_read,
    spawn(fun() -> Me ! {I1, lasp:read(SetId, {strict, V0})} end),

    %% Ensure we receive [1, 2, 3].
    Set1 = receive
        {I1, {ok, {_, _, _, X}}} ->
            ?SET:value(X)
    end,

    %% Perform more inflations.
    {ok, {_, _, _, V1}} = lasp:update(SetId, {add_all, [4]}, actor),
    {ok, _} = lasp:update(SetId, {add_all, [5]}, actor),

    %% Spawn fun which should block until lattice is a strict inflation
    %% of V1.
    I2 = second_read,
    spawn(fun() -> Me ! {I2, lasp:read(SetId, {strict, V1})} end),

    %% Ensure we receive [1, 2, 3, 4].
    Set2 = receive
        {I2, {ok, {_, _, _, Y}}} ->
            ?SET:value(Y)
    end,

    {Set1, Set2}.
