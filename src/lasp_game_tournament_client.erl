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

-module(lasp_game_tournament_client).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {actor, triggers}).

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
    lager:info("Game tournament client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule game enrollment.
    schedule_enrollment(),

    %% Build DAG.
    case lasp_config:get(heavy_client, false) of
        true ->
            build_dag();
        false ->
            ok
    end,

    %% Schedule logging.
    schedule_logging(),

    {ok, #state{actor=Actor, triggers=dict:new()}}.

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
handle_info(log, #state{actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Get current value of the list of enrollments.
    {ok, GameList} = lasp:query(?ENROLLABLE_GAMES),

    lager:info("Game list: ~p, node: ~p", [GameList, Actor]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(view, #state{actor=Actor,
                         triggers=Triggers0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("view"),

    %% Get current value of the list of enrollments.
    {ok, GameList0} = lasp:query(?ENROLLABLE_GAMES),

    %% Make sure we have games...
    Triggers1 = case sets:size(GameList0) of
        0 ->
            %% Do nothing.
            Triggers0;
        Size ->
            %% Select random.
            Random = lasp_support:puniform(Size),

            GameId = lists:nth(Random, sets:to_list(GameList0)),

            %% Spawn a process to disable the game if it goes
            %% above the maximum number of enrollments.
            %%
            Triggers = case lasp_config:get(heavy_client, false) of
                true ->
                    case dict:find(GameId, Triggers0) of
                        {ok, _Pid} ->
                            Triggers0;
                        error ->
                            Pid = spawn_link(fun() -> trigger(GameId, Actor) end),
                            dict:store(GameId, Pid, Triggers0)
                    end;
                false ->
                    Triggers0
            end,

            %% Enroll random player identifier into the game.
            PlayerId = lasp_support:puniform(1000),
            {ok, _} = lasp:update(GameId, {add, PlayerId}, Actor),

            %% Update triggers.
            Triggers
    end,

    {ok, GameList} = lasp:query(?ENROLLABLE_GAMES),
    {ok, {_GamesId, GamesType, _GamesMetadata, GamesValue}} = lasp:read(?ENROLLABLE_GAMES, undefined),

    %% - If there are no games (`sets:size(GameList) == 0')
    %%   it might mean the experiment hasn't started or it has started
    %%   and all games are enrolled.
    %%   If `not lasp_type:is_bottom(GamesType, GamesValue)' then the experiment
    %%   has started. With both, it means the experiement has ended
    %% - Else, keep doing enrollments
    case sets:size(GameList) == 0 andalso not lasp_type:is_bottom(GamesType, GamesValue) of
        true ->
            lager:info("All games are fully enrolled. Node: ~p", [Actor]),

            %% Update Simulation Status Instance
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {fst, true}}, Actor),
            log_convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_enrollment()
    end,

    {noreply, State#state{triggers=Triggers1}};

handle_info(check_simulation_end, #state{actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for clients when all clients have
    %% observed all games enrolled (first component of the map in
    %% the simulation status instance is true for all clients)
    {ok, GamesEnrolledAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithGamesEnrolled = lists:filter(
        fun({_Node, {GamesEnrolled, _LogsPushed}}) ->
            GamesEnrolled
        end,
        GamesEnrolledAndLogs
    ),

    case length(NodesWithGamesEnrolled) == lasp_config:get(client_number, 3) of
        true ->
            lager:info("All nodes observed games enrolled. Node ~p", [Actor]),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {snd, true}}, Actor);
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
schedule_enrollment() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?ENROLLMENT_INTERVAL),
    timer:send_after(?ENROLLMENT_INTERVAL + Jitter, view).

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
build_dag() ->
    %% Nothing to do.
    ok.

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
