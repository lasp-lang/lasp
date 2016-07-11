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

-module(lasp_music_festival_server).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

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

%% Macros.
-define(MAX_IMPRESSIONS, 100).
-define(LOG_INTERVAL, 10000).
-define(CONVERGENCE_INTERVAL, 1000).

%% State record.
-record(state, {actor}).

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
    lager:info("Advertisement counter server initialized."),

    %% Track whether convergence is reached or not.
    lasp_config:set(convergence, false),

    %% Generate actor identifier.
    Actor = self(),

    %% Schedule logging.
    schedule_logging(),

    %% Create instance for convergence tracking
    %% Also schedule check convergence
    track_convergence(),

    {ok, #state{actor=Actor}}.

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
handle_info(log, State) ->
    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(check_convergence, #state{}=State) ->
    {ok, {_, Convergence}} = lasp:query(convergence_id()),

    NodesWithAllEvents = lists:filter(
        fun({_Node, AllEvents}) ->
            AllEvents
        end,
        Convergence
    ),

    case length(NodesWithAllEvents) == client_number() of
        true ->
            lager:info("Convergence reached on all clients"),
            lasp_config:set(convergence, true),
            lasp_transmission_instrumentation:convergence();
        false ->
            schedule_check_convergence()
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
client_number() ->
    ?NUM_NODES.

%% @private
convergence_id() ->
    PairType = {?PAIR_TYPE,
                    [
                        ?COUNTER_TYPE,
                        {?GMAP_TYPE, [?BOOLEAN_TYPE]}
                    ]
                },
    {?CONVERGENCE_TRACKING, PairType}.

%% @private
track_convergence() ->
    {Id, Type} = convergence_id(),
    {ok, _} = lasp:declare(Id, Type),
    schedule_check_convergence().

%% @private
schedule_check_convergence() ->
    erlang:send_after(?CONVERGENCE_INTERVAL, self(), check_convergence).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).
