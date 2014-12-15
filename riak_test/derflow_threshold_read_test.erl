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

-module(derflow_threshold_read_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = derflow_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    {GSet, GSet2} = rpc:call(Node, ?MODULE, test, []),
    ?assertMatch({ok, _, [1,2,3], _}, GSet),
    ?assertMatch({ok, _, [1,2,3,4], _}, GSet2),

    pass.

-endif.

test() ->
    %% Create new set-based CRDT.
    {ok, GSetId} = derflow:declare(riak_dt_gset),

    %% Determine my pid.
    Me = self(),

    %% Spawn fun which should block until lattice reaches [1,2,3].
    spawn(fun() -> Me ! derflow:read(GSetId, [1,2,3]) end),

    %% Perform 4 binds, each an inflation.
    {ok, _} = derflow:bind(GSetId, [1]),
    {ok, _} = derflow:bind(GSetId, [1, 2]),
    {ok, _} = derflow:bind(GSetId, [1, 2, 3]),

    %% Ensure we receive [1, 2, 3].
    GSet = receive
        {ok, _, [1, 2, 3], _} = V ->
            V
    end,

    %% Spawn fun which should block until lattice reaches [1,2,3,4,5].
    spawn(fun() -> Me ! derflow:read(GSetId, [1,2,3,4]) end),

    %% Perform another inflation.
    {ok, _} = derflow:bind(GSetId, [1, 2, 3, 4]),
    {ok, _} = derflow:bind(GSetId, [1, 2, 3, 4, 5]),

    %% Ensure we receive [1, 2, 3, 4].
    GSet2 = receive
        {ok, _, [1, 2, 3, 4], _} = V1 ->
            V1
    end,

    lager:info("GSet: ~p GSet2: ~p", [GSet, GSet2]),

    {GSet, GSet2}.
