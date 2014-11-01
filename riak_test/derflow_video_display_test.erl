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

%% @doc Video display test.

-module(derflow_video_display_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         sender/3]).

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
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], Result),
    pass.

-endif.

test() ->
    {ok, S1} = derflow:declare(),
    spawn(derflow_video_display_test, sender, [0, 10, S1]),
    display(S1),
    derflow:get_stream(S1).

sender(Init, N, Output) ->
    if
        N >= 0 ->
            timer:sleep(500),
            {ok, Next} = derflow:produce(Output, Init),
            sender(Init + 1, N - 1,  Next);
        true ->
            timer:sleep(500),
            derflow:bind(Output, undefined)
    end.

skip1(Input, Output) ->
    case derflow:consume(Input) of
        {ok, undefined, _} ->
            derflow:bind(Output, undefined);
        {ok, _Value, Next} ->
            case derflow:is_det(Next) of
                true ->
                    skip1(Next, Output);
                false ->
                    derflow:bind(Output, {id, Input})
            end
    end.

display(Input) ->
    timer:sleep(1500),
    {ok, Output} = derflow:declare(),
    skip1(Input, Output),
    case derflow:consume(Output) of
        {ok, undefined, _} ->
            ok;
        {ok, Value, Next} ->
            display_frame(Value),
            display(Next)
    end.

display_frame(X) ->
    io:format("Frame: ~w~n",[X]).
