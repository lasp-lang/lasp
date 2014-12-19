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

%% @doc Bounded buffer test application.

-module(derpflow_bounded_buffer_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         buffer/3,
         producer/3,
         consumer/4]).

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
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual([0,2,4], Result),
    pass.

-endif.

test() ->
    {ok, S1} = derpflow:declare(),
    spawn(derpflow_bounded_buffer_test, producer, [0, 3, S1]),
    {ok, S2} = derpflow:declare(),
    spawn(derpflow_bounded_buffer_test, buffer, [S1, 2, S2]),
    {ok, S3} = derpflow:declare(),
    consumer(S2, 5, fun(X) -> X*2 end, S3),
    derpflow:get_stream(S3).

producer(Value, N, Output) ->
    if
        (N > 0) ->
            {ok, _} = derpflow:wait_needed(Output),
            {ok, Next} = derpflow:produce(Output, Value),
            producer(Value + 1, N - 1,  Next);
        true ->
            derpflow:bind(Output, undefined)
    end.

loop(S1, S2, End) ->
    {ok, _} = derpflow:wait_needed(S2),
    {ok, _, S1Value, S1Next} = derpflow:consume(S1),
    {ok, S2Next} = derpflow:produce(S2, S1Value),
    case derpflow:produce(S2, S1Value) of
        {ok, undefined} ->
            ok;
        {ok, S2Next} ->
            case derpflow:extend(End) of
                {ok, {undefined, _}} ->
                    ok;
                {ok, EndNext} ->
                    loop(S1Next, S2Next, EndNext)
            end
    end.

buffer(S1, Size, S2) ->
    End = drop_list(S1, Size),
    loop(S1, S2, End).

drop_list(S, Size) ->
    if
        Size == 0 ->
            S;
        true ->
            {ok, Next} = derpflow:extend(S),
            drop_list(Next, Size - 1)
    end.

consumer(S2, Size, F, Output) ->
    case Size of
        0 ->
            ok;
        _ ->
            case derpflow:consume(S2) of
                {ok, _, undefined, _} ->
                    derpflow:bind(Output, undefined);
                {ok, _, Value, Next} ->
                    {ok, NextOutput} = derpflow:produce(Output, F(Value)),
                    consumer(Next, Size - 1, F, NextOutput)
            end
    end.
