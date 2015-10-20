%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Christopher S. Meiklejohn.  All Rights Reserved.
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

%% @doc Leaderboard example.

-module(lasp_leaderboard_test).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([test/0,
         client/4]).

-ifdef(TEST).

-export([confirm/0]).

-define(HARNESS, (rt_config:get(rt_harness))).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Nodes] = lasp_test_helpers:build_clusters([1]),
    lager:info("Nodes: ~p", [Nodes]),
    Node = hd(Nodes),

    lager:info("Remotely loading code on node ~p", [Node]),
    ok = lasp_test_helpers:load(Nodes),
    lager:info("Remote code loading complete."),

    ok = lasp_test_helpers:wait_for_cluster(Nodes),

    lager:info("Remotely executing the test."),
    Result = rpc:call(Node, ?MODULE, test, []),
    ?assertEqual(ok, Result),

    pass.

-endif.

-define(SET, lasp_orset).

%% The number of clients.
-define(NUM_CLIENTS, 10).

%% The number of events to sent to clients.
-define(NUM_EVENTS, 2000).

test() ->
    Self = self(),

    %% Create a leaderboard datatype.
    {ok, LeaderboardId} = lasp:declare({lasp_top_k_var, [2]}),

    %% Read the leaderboard's current value.
    {ok, {_, _, _, Leaderboard}} = lasp:read(LeaderboardId, undefined),

    %% Launch client processes.
    ClientList = clients(Self, LeaderboardId, Leaderboard),

    %% Initialize simulation.
    simulate(Self, ClientList),

    %% Wait until we receive num events.
    FinalResult = wait_for_events(1, ?NUM_EVENTS, 0),

    %% Terminate all clients.
    terminate(ClientList),

    %% Read the result and print it.
    {ok, FinalLeaderboard} = lasp:query(LeaderboardId),
    Final = orddict:to_list(FinalLeaderboard),

    %% Assert we got the right score.
    [{_, FinalResult}, {_, _}] = Final,

    io:format("Final Leaderboard: ~p", [Final]),

    ok.

%% @doc Terminate any running clients gracefully issuing final
%%      synchronization.
terminate(ClientList) ->
    TerminateFun = fun(Pid) -> Pid ! terminate end,
    lists:map(TerminateFun, ClientList).

%% @doc Launch a series of client processes.
clients(Runner, LeaderboardId, Leaderboard) ->
    SpawnFun = fun(Id) -> spawn_link(?MODULE, client, [Runner, Id, LeaderboardId, Leaderboard]) end,
    lists:map(SpawnFun, lists:seq(1, ?NUM_CLIENTS)).

%% @doc Client process; standard recurisve looping server.
client(Runner, Id, LeaderboardId, Leaderboard0) ->
    receive
        {complete_game, Score} ->
            io:format("Client ~p completed game with score ~p.", [Id, Score]),

            %% Update local leaderboard.
            {ok, Leaderboard} = lasp_top_k_var:update({set, Id, Score}, Id, Leaderboard0),

            %% Notify the harness that an event has been processed.
            Runner ! {event, Score},

            client(Runner, Id, LeaderboardId, Leaderboard);
        terminate ->
            io:format("Client ~p shutting down, issuing final synchronization.", [Id]),

            %% Synchronize copy of leaderboard.
            {ok, {_, _, _, Leaderboard}} = lasp:bind(LeaderboardId, Leaderboard0),

            ok
    after
        10 ->
            io:format("Client ~p synchronizing leaderboard.", [Id]),

            %% Synchronize copy of leaderboard.
            {ok, {_, _, _, Leaderboard}} = lasp:bind(LeaderboardId, Leaderboard0),

            client(Runner, Id, LeaderboardId, Leaderboard)
    end.

%% @doc Simulate clients.
simulate(_Runner, ClientList) ->
    %% Start the simulation.
    Viewer = fun(_) ->
            %% Pick a random client.
            Random = random:uniform(length(ClientList)),
            Pid = lists:nth(Random, ClientList),

            %% Sleep to simulate game run time.
            timer:sleep(5),

            %% This simulates a game being completed on a clients
            %% device.
            Pid ! {complete_game, random:uniform(100000000)}
    end,
    lists:foreach(Viewer, lists:seq(1, ?NUM_EVENTS)).

%% @doc Wait for all events to be delivered in the system.
wait_for_events(Count, NumEvents, MaxValue0) ->
    receive
        {event, Score} ->
            MaxValue = max(Score, MaxValue0),
            case Count >= NumEvents of
                true ->
                    io:format("~p events served, max is: ~p!",
                              [Count, MaxValue]),
                    MaxValue;
                false ->
                    wait_for_events(Count + 1, NumEvents, MaxValue)
            end
    end.
