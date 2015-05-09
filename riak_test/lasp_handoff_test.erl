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

-export([declare/1,
         read/1]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    % Configure a cluster with 2 nodes
    [Nodes] = rt:build_clusters([2]),
    lager:info("Nodes: ~p", [Nodes]),
    [N1, N2] = Nodes,

    lager:info("Remotely loading code on node ~p", [N1]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Declaring variables."),
    {ok, Ids} = rpc:call(N1, ?MODULE, declare, [1]),

    lager:info("Ids: ~p", [Ids]),

    % Remove a node from the cluster (Handoff will be perform)
    lager:info("Removing node ~p from cluster", [N1]),
    rt:leave(N1),
    ?assertEqual(ok, rt:wait_until_unpingable(N1)),

    lager:info("Reading variables."),
    {ok, [1,1,1]} = rpc:call(N2, ?MODULE, read, [Ids]),

    lager:info("Done!"),

    pass.
-endif.

declare(V1) ->
    % Declare variables
    {ok, I1} = lasp:declare(lasp_ivar),
    {ok, I2} = lasp:declare(lasp_ivar),
    {ok, I3} = lasp:declare(lasp_ivar),

    {ok, _} = lasp:bind_to(I2, I1),
    {ok, _} = lasp:bind(I1, V1),
    {ok, _} = lasp:bind_to(I3, I1),

    % Read variables
    {ok, {_, _, V1, _}} = lasp:read(I3),
    {ok, {_, _, V1, _}} = lasp:read(I2),
    {ok, {_, _, V1, _}} = lasp:read(I1),

    L1 = lists:append([[I1], [I2], [I3]]),

    % Return
    {ok, L1}.

read(Ids) ->
    Values = lists:map(fun(Id) ->
                {ok, {_, _, V1, _}} = lasp:read(Id),
                V1
        end, Ids),
    {ok, Values}.
