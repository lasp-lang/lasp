%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(lasp_game_tournament_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         trigger/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {actor, game_list}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    lager:info("Game tournament server initialized."),

    %% Delay for graph connectedness.
    wait_for_connectedness(),
    lasp_instrumentation:experiment_started(),

    %% Track whether simulation has ended or not.
    lasp_config:set(simulation_end, false),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule logging.
    schedule_logging(),

    %% Build DAG.
    {ok, GameList} = build_dag(Actor),

    %% Initialize triggers.
    launch_triggers(GameList, Actor),

    %% Create instance for simulation status tracking
    {Id, Type} = ?SIM_STATUS_ID,
    {ok, _} = lasp:declare(Id, Type),

    %% Schedule check simulation end
    schedule_check_simulation_end(),

    {ok, #state{actor=Actor, game_list=GameList}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(log, #state{}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Print number of games.
    {ok, Games} = lasp:query(?ENROLLABLE_GAMES),

    lager:info("Game list: ~p", [sets:size(Games)]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(check_simulation_end, #state{game_list=GameList}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for the server when all clients have
    %% observed that all clients observed all games enrolled and
    %% pushed their logs (second component of the map in
    %% the simulation status instance is true for all clients)
    {ok, GamesEnrolledAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithLogsPushed = lists:filter(
        fun({_Node, {_GamesEnrolled, LogsPushed}}) ->
            LogsPushed
        end,
        GamesEnrolledAndLogs
    ),

    NodesWithGamesEnrolled = lists:filter(
        fun({_Node, {GamesEnrolled, _LogsPushed}}) ->
            GamesEnrolled
        end,
        GamesEnrolledAndLogs
    ),

    lager:info("Checking for simulation end: ~p nodes with enrolled games and ~p nodes with logs pushed.",
               [length(NodesWithGamesEnrolled), length(NodesWithLogsPushed)]),

    case length(NodesWithLogsPushed) == lasp_config:get(client_number, 3) of
        true ->
            lager:info("All nodes have pushed their logs"),
            log_overcounting_and_convergence(GameList),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp_config:set(simulation_end, true),
            stop_simulation();
        false ->
            schedule_check_simulation_end()
    end,

    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
build_dag(Actor) ->
    %% Create a bunch of unique identifiers for games.
    GameIds = lists:map(fun(_) ->
                                {ok, Unique} = lasp_unique:unique(),
                                Unique
                        end, lists:seq(1, ?GAMES_NUMBER)),

    %% Create each game, and then add each of the games to the set of
    %% enrollable games.
    GameList = lists:map(fun(Id) ->
                    %% Generate identifier.
                    GameId = {Id, ?GSET_TYPE},

                    %% Generate game.
                    {ok, {GameId, _, _, _}} = lasp:declare(GameId, ?GSET_TYPE),

                    %% Add game.
                    {ok, _} = lasp:update(?ENROLLABLE_GAMES,
                                          {add, GameId},
                                          Actor),

                    GameId

                    end, GameIds),

    {ok, GameList}.

%% @private
launch_triggers(GameList, Actor) ->
    lists:map(
        fun(GameId) ->
            spawn_link(
                fun() ->
                    trigger(GameId, Actor)
                end
            )
        end,
        GameList
    ).

%% @private
trigger(GameId, Actor) ->
    %% Blocking threshold read for max players.
    MaxPlayers = lasp_config:get(max_players, ?MAX_PLAYERS_DEFAULT),

    EnforceFun = fun() ->
            lager:info("Threshold for ~p reached; disabling!", [GameId]),

            %% Remove the game.
            {ok, _} = lasp:update(?ENROLLABLE_GAMES, {rmv, GameId}, Actor)
    end,
    lasp:invariant(GameId, {cardinality, MaxPlayers}, EnforceFun),

    ok.

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
log_overcounting_and_convergence(GameList) ->
    case lasp_config:get(instrumentation, false) of
        true ->
            Overcounting = compute_overcounting(GameList),
            lasp_instrumentation:overcounting(Overcounting),
            lasp_instrumentation:convergence();

        false ->
            ok
    end.

%% @private
compute_overcounting(GameList) ->
    OvercountingSum = lists:foldl(
        fun(GameId, Acc) ->
            {ok, Value} = lasp:query(GameId),
            MaxPlayers = lasp_config:get(max_players, ?MAX_PLAYERS_DEFAULT),
            %% Size, because we count cardinality.
            Overcounting = sets:size(Value) - MaxPlayers,
            lager:info("Game ~p had ~p enrolled out of ~p",
                       [GameId, sets:size(Value), MaxPlayers]),
            OvercountingPercentage = (Overcounting * 100) / MaxPlayers,
            Acc + OvercountingPercentage
        end,
        0,
        GameList
    ),

    OvercountingSum / length(GameList).

%% @private
stop_simulation() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            lasp_marathon_simulations:stop()
    end.

%% @private
wait_for_connectedness() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            case sprinter_backend:was_connected() of
                {ok, true} ->
                    ok;
                {ok, false} ->
                    timer:sleep(100),
                    wait_for_connectedness()
            end
    end.
