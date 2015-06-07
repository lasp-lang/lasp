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

-module(lasp_bind_test).
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
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    ?assertEqual(ok, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

test() ->
    test_ivars(),
    test_lattice(lasp_gset),
    test_lattice(lasp_orset),
    test_lattice(lasp_orset_gbtree),
    test_lattice(lasp_orswot),
    ok.

%% @doc Test single-assignment variables.
test_ivars() ->
    %% Single-assignment variables.
    {ok, I1} = lasp:declare(lasp_ivar),
    {ok, I2} = lasp:declare(lasp_ivar),
    {ok, I3} = lasp:declare(lasp_ivar),

    V1 = 1,

    %% Attempt pre, and post- dataflow variable bind operations.
    ok = lasp:bind_to(I2, I1),
    {ok, _} = lasp:bind(I1, V1),
    ok = lasp:bind_to(I3, I1),

    %% Perform invalid bind; won't return error, just will have no
    %% effect.
    {ok, _} = lasp:bind(I1, 2),

    %% Verify the same value is contained by all.
    {ok, {_, _, V1}} = lasp:read(I3, {strict, undefined}),
    {ok, {_, _, V1}} = lasp:read(I2, {strict, undefined}),
    {ok, {_, _, V1}} = lasp:read(I1, {strict, undefined}),

    ok.

%% @doc Test lattice-based variables.
test_lattice(Type) ->
    %% G-Set variables.
    {ok, L1} = lasp:declare(Type),
    {ok, L2} = lasp:declare(Type),
    {ok, L3} = lasp:declare(Type),

    %% Attempt pre, and post- dataflow variable bind operations.
    ok = lasp:bind_to(L2, L1),
    {ok, _} = lasp:update(L1, {add, 1}, a),
    ok = lasp:bind_to(L3, L1),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, S1}} = lasp:read(L3, {strict, undefined}),
    {ok, {_, _, S1}} = lasp:read(L2, {strict, undefined}),
    {ok, {_, _, S1}} = lasp:read(L1, {strict, undefined}),

    %% Test inflations.
    {ok, S2} = Type:update({add, 2}, a, S1),

    Self = self(),

    lager:info("About to spawn wait_needed function..."),
    spawn_link(fun() ->
                  {ok, _} = lasp:wait_needed(L1, {strict, S1}),
                  Self ! threshold_met
               end),

    {ok, _} = lasp:bind(L1, S2),

    timer:sleep(4000),

    %% Verify the same value is contained by all.
    {ok, {_, _, S2}} = lasp:read(L3, {strict, undefined}),
    {ok, {_, _, S2}} = lasp:read(L2, {strict, undefined}),
    {ok, {_, _, S2}} = lasp:read(L1, {strict, undefined}),

    %% Read at the S2 threshold level.
    {ok, {_, _, S2}} = lasp:read(L1, S2),

    %% Wait for wait_needed to unblock.
    receive
        threshold_met ->
            ok
    end,

    {ok, L5} = lasp:declare(Type),
    {ok, L6} = lasp:declare(Type),

    lager:info("About to spawn read_any function..."),
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
