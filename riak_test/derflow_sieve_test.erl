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

%% @doc Test sieve example.

-module(derpflow_sieve_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/1,
         sieve/2,
         filter/3,
         generate/3]).

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
    Result = rpc:call(Node, ?MODULE, test, [100]),
    ?assertEqual([2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,
                 61,67,71,73,79,83,89,97], Result),
    pass.

-endif.

test(Max) ->
    {ok, S1} = derpflow:declare(),
    spawn(derpflow_sieve_test, generate, [2, Max, S1]),
    {ok, S2} = derpflow:declare(),
    spawn(derpflow_sieve_test, sieve, [S1, S2]),
    derpflow:get_stream(S2).

sieve(S1, S2) ->
    case derpflow:consume(S1) of
        {ok, _, undefined, _} ->
            derpflow:bind(S2, undefined);
        {ok, _, Value, Next} ->
            {ok, SN} = derpflow:declare(),
            spawn(derpflow_sieve_test, filter,
                           [Next, fun(Y) -> Y rem Value =/= 0 end, SN]),
            {ok, NextOutput} = derpflow:produce(S2, Value),
            sieve(SN, NextOutput)
    end.

filter(S1, F, S2) ->
    case derpflow:consume(S1) of
        {ok, _, undefined, _} ->
            derpflow:bind(S2, undefined);
        {ok, _, Value, Next} ->
            case F(Value) of
                false ->
                    filter(Next, F, S2);
                true->
                    {ok, NextOutput} = derpflow:produce(S2, Value),
                    filter(Next, F, NextOutput)
            end
    end.

generate(Init, N, Output) ->
    if
        (Init =< N) ->
            timer:sleep(250),
            {ok, Next} = derpflow:produce(Output, Init),
            generate(Init + 1, N,  Next);
        true ->
            derpflow:bind(Output, undefined)
    end.
