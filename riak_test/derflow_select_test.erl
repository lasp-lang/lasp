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

%% @doc Select test.

-module(derpflow_select_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = rt:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = derpflow_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = derpflow_test_helpers:wait_for_cluster(Nodes),

    ?assertEqual({ok, [1,2,3,4,5,6], [2,4,6]},
                 rpc:call(Node, ?MODULE, test, [riak_dt_gset])),

    lager:info("Done!"),

    pass.

-endif.

test(Type) ->
    %% Create initial set.
    {ok, S1} = derpflow:declare(Type),

    %% Add elements to initial set.
    {ok, _, S1V1, _} = derpflow:read(S1),
    {ok, S1V2} = Type:update({add_all, [1,2,3]}, undefined, S1V1),

    %% Bind update.
    {ok, _} = derpflow:bind(S1, S1V2),

    %% Read resulting value.
    {ok, _, S1V2, _} = derpflow:read(S1),

    %% Create second set.
    {ok, S2} = derpflow:declare(Type),

    %% Apply select.
    {ok, _Pid} = derpflow:select(S1, fun(X) -> X rem 2 == 0 end, S2),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    {ok, _, S1V3, _} = derpflow:read(S1),
    {ok, S1V4} = Type:update({add_all, [4,5,6]}, undefined, S1V3),
    {ok, _} = derpflow:bind(S1, S1V4),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, _, S1V4, _} = derpflow:read(S1),

    %% Read resulting value.
    {ok, _, S2V1, _} = derpflow:read(S2),

    {ok, S1V4, S2V1}.
