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

%% @doc Test that a bind works on a distributed cluster of nodes.

-module(derflow_bind_test).
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
    ?assertEqual(ok, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

test() ->
    {ok, Id1} = derflow:declare(),

    {ok, Id2} = derflow:declare(),

    {ok, _} = derflow:bind_to(Id2, Id1),
    lager:info("Successful bind."),

    {ok, _} = derflow:bind(Id1, 1),
    lager:info("Successful bind."),

    {ok, _, Value1, _} = derflow:read(Id1),
    lager:info("Successful read: ~p", [Value1]),

    error = derflow:bind(Id1, 2),
    lager:info("Unsuccessful bind."),

    {ok, _, Value1, _} = derflow:read(Id1),
    lager:info("Successful read: ~p", [Value1]),

    {ok, _, Value1, _} = derflow:read(Id2),
    lager:info("Successful read: ~p", [Value1]),

    {ok, Id3} = derflow:declare(),

    {ok, _} = derflow:bind_to(Id3, Id1),
    lager:info("Successful bind."),

    {ok, _, Value1, _} = derflow:read(Id3),
    lager:info("Successful read: ~p", [Value1]),

    ok.
