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

%% @doc Lattice test.

-module(lasp_lattice_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         producer/3,
         consumer/3]).

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
    ?assertEqual({ok, [[0],
                       [0,1],
                       [0,1,2],
                       [0,1,2,3],
                       [0,1,2,3,4],
                       [0,1,2,3,4,5],
                       [0,1,2,3,4,5,6],
                       [0,1,2,3,4,5,6,7],
                       [0,1,2,3,4,5,6,7,8],
                       [0,1,2,3,4,5,6,7,8,9]]}, rpc:call(Node, ?MODULE, test, [])),

    lager:info("Done!"),

    pass.

-endif.

test() ->
    %% Generate a stream of objects.
    {ok, ObjectStream} = lasp:declare(),
    lasp:thread(?MODULE, producer, [0, 10, ObjectStream]),

    %% Accumulate the objects into a set.
    {ok, ObjectSetStream} = lasp:declare(),
    {ok, ObjectSetId} = lasp:declare(riak_dt_gset),
    ObjectSetFun = fun(X) ->
            lager:info("~p set received: ~p", [self(), X]),
            {ok, _, Set0, _} = lasp:read(ObjectSetId),
            {ok, Set} = riak_dt_gset:update({add, X}, undefined, Set0),
            {ok, _} = lasp:bind(ObjectSetId, Set),
            lager:info("~p set bound to new set: ~p", [self(), Set]),
            Set
    end,
    lasp:thread(?MODULE, consumer, [ObjectStream, ObjectSetFun, ObjectSetStream]),

    %% Block until all operations are complete, to ensure we don't shut
    %% the test harness down until everything is computed.
    ObjectSetStreamValues = lasp:get_stream(ObjectSetStream),
    lager:info("Retrieving set stream: ~p",
               [ObjectSetStreamValues]),

    {ok, ObjectSetStreamValues}.

%% @doc Stream producer, which generates a series of inputs on a stream.
producer(Init, N, Output) ->
    case N > 0 of
        true ->
            timer:sleep(1000),
            {ok, Next} = lasp:produce(Output, Init),
            producer(Init + 1, N-1,  Next);
        false ->
            lasp:bind(Output, nil)
    end.

%% @doc Stream consumer, which accepts inputs on one stream, applies a
%%      function, and then produces inputs on another stream.
consumer(S1, F, S2) ->
    case lasp:consume(S1) of
        {ok, _, nil, _} ->
            lager:info("~p consumed: ~p", [self(), nil]),
            lasp:bind(S2, nil);
        {ok, _, Value, Next} ->
            lager:info("~p consumed: ~p", [self(), Value]),
            Me = self(),
            spawn(fun() -> Me ! F(Value) end),
            NewValue = receive
                X ->
                    X
            end,
            {ok, NextOutput} = lasp:produce(S2, NewValue),
            consumer(Next, F, NextOutput)
    end.
