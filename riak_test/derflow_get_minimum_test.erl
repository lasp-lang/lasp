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

%% @doc Test that streaming bind of an insertion sort works.

-module(derpflow_get_minimum_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1,
         insort/2,
         insert/3]).

-define(TABLE, minimum).

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

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, [[1,2,3,4,5]]),
    ?assertEqual(1, Result),
    pass.

-endif.

test(List) ->
    {ok, S1} = derpflow:declare(),
    ?TABLE = ets:new(?TABLE, [set, named_table, public, {write_concurrency, true}]),
    true = ets:insert(?TABLE, {count, 0}),
    spawn(derpflow_get_minimum_test, insort, [List, S1]),
    {ok, _, V, _} = derpflow:consume(S1),
    V.

insort(List, S) ->
    case List of
        [H|T] ->
            {ok, OutS} = derpflow:declare(),
            insort(T, OutS),
            spawn(derpflow_get_minimum_test, insert, [H, OutS, S]);
        [] ->
            derpflow:bind(S, undefined)
    end.

insert(X, In, Out) ->
    [{Id, C}] = ets:lookup(?TABLE, count),
    true = ets:insert(?TABLE, {Id, C+1}),
    {ok, _} = derpflow:wait_needed(Out),
    case derpflow:consume(In) of
        {ok, _, undefined, _} ->
            {ok, Next} = derpflow:produce(Out, X),
            derpflow:bind(Next, undefined);
        {ok, _, V, SNext} ->
            if
                X < V ->
                    {ok, Next} = derpflow:produce(Out, X),
                    derpflow:bind(Next, In);
                true ->
                    {ok, Next} = derpflow:produce(Out, V),
                    insert(X, SNext, Next)
            end
    end.
