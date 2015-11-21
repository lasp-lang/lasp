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

-module(lasp_leaderboard).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([run/0,
         client/4]).

-behaviour(lasp_simulation).

%% lasp_simulation callbacks
-export([init/0,
         clients/1,
         simulate/1,
         wait/1,
         terminate/1,
         summarize/1]).

run() ->
    lasp_simulation:run(?MODULE).

%% Macro definitions.

%% Lasp set type to use.
-define(SET, lasp_orset).

%% The number of clients.
-define(NUM_CLIENTS, 10).

%% The number of events to sent to clients.
-define(NUM_EVENTS, 2000).

%% Record definitions.
-record(state, {runner,
                client_list,
                count_events = 1,
                num_events = ?NUM_EVENTS,
                leaderboard,
                leaderboard_id}).

%% @doc Preconfigure the example.
init() ->
    Runner = self(),

    %% Create a leaderboard datatype.
    {ok, {LeaderboardId, _, _, _}} = lasp:declare({lasp_top_k_var, [2]}),

    %% Read the leaderboard's current value.
    {ok, {_, _, _, Leaderboard}} = lasp:read(LeaderboardId, undefined),

    {ok, #state{runner=Runner,
                leaderboard_id=LeaderboardId,
                leaderboard=Leaderboard}}.

summarize(#state{leaderboard_id=LeaderboardId}=State) ->
    %% Read the result and print it.
    {ok, FinalLeaderboard} = lasp:query(LeaderboardId),
    Final = orddict:to_list(FinalLeaderboard),
    io:format("Final Leaderboard: ~p", [Final]),

    {ok, State}.

%% @doc Terminate any running clients gracefully issuing final
%%      synchronization.
terminate(#state{client_list=ClientList}=State) ->
    TerminateFun = fun(Pid) -> Pid ! terminate end,
    lists:map(TerminateFun, ClientList),
    {ok, State}.

%% @doc Launch a series of client processes.
clients(#state{runner=Runner,
               leaderboard_id=LeaderboardId,
               leaderboard=Leaderboard}=State) ->
    SpawnFun = fun(Id) ->
                       spawn_link(?MODULE,
                                  client,
                                  [Runner, Id, LeaderboardId, Leaderboard])
               end,
    ClientList = lists:map(SpawnFun, lists:seq(1, ?NUM_CLIENTS)),
    {ok, State#state{client_list=ClientList}}.

%% @doc Client process; standard recurisve looping server.
client(Runner, Id, LeaderboardId, Leaderboard0) ->
    receive
        {complete_game, Score} ->
            %% Update local leaderboard.
            {ok, Leaderboard} = lasp_top_k_var:update({set, Id, Score}, Id, Leaderboard0),

            %% Notify the harness that an event has been processed.
            Runner ! {event, Score},

            client(Runner, Id, LeaderboardId, Leaderboard);
        terminate ->
            %% Synchronize copy of leaderboard.
            {ok, {_, _, _, Leaderboard}} = lasp:bind(LeaderboardId, Leaderboard0),

            ok
    after
        10 ->
            %% Synchronize copy of leaderboard.
            {ok, {_, _, _, Leaderboard}} = lasp:bind(LeaderboardId, Leaderboard0),

            client(Runner, Id, LeaderboardId, Leaderboard)
    end.

%% @doc Simulate clients.
simulate(#state{client_list=ClientList}=State) ->
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
    lists:foreach(Viewer, lists:seq(1, ?NUM_EVENTS)),
    {ok, State}.

%% @doc Wait for all events to be delivered in the system.
wait(#state{count_events=Count, num_events=NumEvents}=State) ->
    receive
        {event, _Score} ->
            case Count >= NumEvents of
                true ->
                    {ok, State};
                false ->
                    wait(State#state{count_events=Count + 1})
            end
    end.
