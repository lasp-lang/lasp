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

-module(lasp_advertisement_counter_client).
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
-define(IMPRESSION_INTERVAL, 10000).
-define(LOG_INTERVAL, 10000).

%% State record.
-record(state, {actor, impressions}).

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
    lager:info("Advertisement counter client initialized."),

    %% Generate actor identifier.
    Actor = self(),

    %% Schedule advertisement counter impression.
    schedule_impression(),

    %% Schedule logging.
    schedule_logging(),

    {ok, #state{actor=Actor, impressions=0}}.

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
handle_info(log, #state{impressions=Impressions}=State) ->
    %% Get current value of the list of advertisements.
    {ok, Ads} = lasp:query({?ADS_WITH_CONTRACTS, ?SET_TYPE}),
    AdList = sets:to_list(Ads),

    lager:info("Impressions: ~p", [Impressions]),

    lager:info("Current advertisement list size: ~p", [length(AdList)]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};
handle_info(view, #state{actor=Actor, impressions=Impressions}=State0) ->
    %% Get current value of the list of advertisements.
    {ok, Ads} = lasp:query({?ADS_WITH_CONTRACTS, ?SET_TYPE}),

    %% Make sure we have ads...
    State = case sets:size(Ads) of
        0 ->
            %% Do nothing.
            State0;
        Size ->
            %% Select random.
            Random = lasp_support:puniform(Size),

            %% @todo Exposes internal details of record.
            {{ad, _, _, Counter},
             _Contract} = lists:nth(Random, sets:to_list(Ads)),

            %% Increment counter.
            {ok, _} = lasp:update(Counter, increment, Actor),

            %% Increment impressions.
            State0#state{impressions=Impressions+1}
    end,

    %% Schedule advertisement counter impression.
    schedule_impression(),

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
schedule_impression() ->
    %% Add random jitter.
    Jitter = random:uniform(?IMPRESSION_INTERVAL),
    erlang:send_after(?IMPRESSION_INTERVAL + Jitter, self(), view).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).
