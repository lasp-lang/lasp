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

%% @doc Fold test.

-module(lasp_fold_test).
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
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    ?assertEqual({ok, [1,2,3,4,5,6], [{1,2},{2,4},{3,6},{4,8},{5,10},{6,12}]},
                 rpc:call(Node, ?MODULE, test, [lasp_orset])),

    ?assertEqual({ok, [1,2,3,4,5,6], [{1,2},{2,4},{3,6},{4,8},{5,10},{6,12}]},
                 rpc:call(Node, ?MODULE, test, [lasp_orset_gbtree])),

    lager:info("Done!"),

    pass.

-endif.

test(Type) ->
    %% Create initial set.
    {ok, S1} = lasp:declare(Type),

    %% Add elements to initial set and update.
    {ok, _} = lasp:update(S1, {add_all, [1,2,3]}, a),

    %% Create second set.
    {ok, S2} = lasp:declare(Type),

    %% Apply fold.
    ok = lasp:fold(S1, fun(X) -> [{X, X*2}] end, S2),

    %% Wait.
    timer:sleep(4000),

    %% Bind again.
    {ok, _} = lasp:update(S1, {add_all, [4,5,6]}, a),

    %% Wait.
    timer:sleep(4000),

    %% Read resulting value.
    {ok, {_, _, _, S1V4}} = lasp:read(S1, {strict, undefined}),

    %% Read resulting value.
    {ok, {_, _, _, S2V1}} = lasp:read(S2, {strict, undefined}),

    {ok, Type:value(S1V4), Type:value(S2V1)}.
