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

%% @doc Test ports example.

-module(lasp_monitor_ports_test).
-author("Christopher Meiklejohn <cmeiklejohn@basho.com>").

-export([test/0,
         run_port/1,
         sensor/2,
         dcs_monitor/3,
         register_comfailure/2,
         acc_register_comfailure/3]).

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
    rpc:call(Node, ?MODULE, test, []),
    pass.

-endif.

test()->
    {ok, S1} = lasp:declare(lasp_ivar),
    Port = spawn(?MODULE, run_port ,[S1]),
    spawn(?MODULE, sensor, [Port, dc_1]),
    spawn(?MODULE, sensor, [Port, dc_2]),
    spawn(?MODULE, sensor, [Port, dc_3]),
    {ok, S2} = lasp:declare(lasp_ivar),
    spawn(?MODULE, dcs_monitor, [S1, S2, []]).

run_port(Stream) ->
    receive
        {Message, From} ->
            {ok, Next} = lasp:produce(Stream, {Message, From}),
            run_port(Next)
    end.

sensor(Port, Identifier) ->
    Miliseconds = round(timer:seconds(random:uniform())),
    timer:sleep(Miliseconds * 10),
    Port ! {computer_down, Identifier},
    sensor(Port, Identifier).

dcs_monitor(Input, Output, State) ->
    case lasp:consume(Input) of
        {ok, _, {computer_down, Identifier}, NextInput} ->
            NewState = register_comfailure(Identifier, State),
            {ok, NextOutput} = lasp:produce(Output, NewState),
            dcs_monitor(NextInput, NextOutput, NewState);
        {ok, _, _, NextInput} ->
            dcs_monitor(NextInput, Output, State)
    end.

register_comfailure(Identifier, State) ->
    acc_register_comfailure(Identifier, State, []).

acc_register_comfailure(Identifier, [], NewState) ->
    lists:append(NewState, [{Identifier, 1}]);
acc_register_comfailure(Identifier, [Next|Rest], NewState) ->
    case Next of
        {Identifier, Counter} ->
            UpdatedPartialList = lists:append(NewState,[{Identifier, Counter+1}]),
            lists:append(UpdatedPartialList, Rest);
        _ ->
            acc_register_comfailure(Identifier, Rest, lists:append(NewState, [Next]))
            end.
