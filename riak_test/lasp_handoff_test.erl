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

%% @doc Test that a handoff works on a distributed cluster of nodes.

-module(lasp_handoff_test).
-author("Loïc Schils <loic.schils@student.uclouvain.be>").

-export([declare/0,
         test/0]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    % Configure a cluster with 2 nodes
    [Nodes] = rt:build_clusters([2]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    ?assertEqual({ok, [4, 8, 8, 4]},
                 rpc:call(Node, ?MODULE, declare, [])),

    % Remove a node from the cluster (Handoff will be perform)
    lager:info("Removing node ~p from cluster", [Node]),
    rt:leave(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),

    OtherNode = lists:last(Nodes),
    ?assertEqual({ok},
                rpc:call(OtherNode, ?MODULE, test, [])),

    pass.
-endif.

declare() ->
    % Declare variables
    {ok, I1} = lasp:declare(lasp_ivar),
    {ok, I2} = lasp:declare(lasp_ivar),
    {ok, I3} = lasp:declare(lasp_ivar),
    {ok, I4} = lasp:declare(lasp_ivar),

    % Bind variables
    V1 = 4,
    V2 = 8,

    {ok, _} = lasp:bind_to(I4, I1),
    {ok, _} = lasp:bind(I1, V1),
    {ok, _} = lasp:bind(I2, V2),
    {ok, _} = lasp:bind_to(I3, I2),

    % Read variables
    {ok, {_, _, R1, _}} = lasp:read(I1),
    {ok, {_, _, R2, _}} = lasp:read(I2),
    {ok, {_, _, R3, _}} = lasp:read(I3),
    {ok, {_, _, R4, _}} = lasp:read(I4),

    L1 = lists:append([[R1], [R2], [R3], [R4]]),

    % Return
    {ok, L1}.

test() ->

    %TODO

    {ok}.
