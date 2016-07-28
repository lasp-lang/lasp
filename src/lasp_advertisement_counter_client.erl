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

    lager:info("Impressions: ~p", [Impressions]),

    lager:info("Current advertisement list size: ~p", [sets:size(Ads)]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(view, #state{actor=Actor, impressions=Impressions0}=State) ->
    %% Get current value of the list of advertisements.
    {ok, Ads} = lasp:query({?ADS_WITH_CONTRACTS, ?SET_TYPE}),

    %% Make sure we have ads...
    Impressions1 = case sets:size(Ads) of
        0 ->
            %% Do nothing.
            Impressions0;
        Size ->
            %% Select random.
            Random = lasp_support:puniform(Size),

            %% @todo Exposes internal details of record.
            {{ad, _, _, Counter},
             _Contract} = lists:nth(Random, sets:to_list(Ads)),

            %% Increment counter.
            {ok, _} = lasp:update(Counter, increment, Actor),

            %% Increment impressions.
            Impressions0 + 1
    end,

	  %% - If I did nothing (`Impressions0 == Impressions1`), 
    %% and the `Impressions1 > 0` (meaning I did something before)
    %% then all ads are disabled and simulation has ended
    %%      (If we don't check for `Impressions1 > 0` it might mean that 
    %%      the experiment hasn't started yet)
    %% - Else, keep doing impressions
    case Impressions0 == Impressions1 andalso Impressions1 > 0 of
        true ->
            lager:info("All ads are disabled. Node: ~p", [node()]),

            %% Update Simulation Status Instance
            lasp:update(?SIM_STATUS_ID, {Actor, {fst, true}}, Actor),
            lasp_transmission_instrumentation:convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_impression()
    end,

    {noreply, State#state{impressions=Impressions1}};

handle_info(check_simulation_end, #state{actor=Actor}=State) ->
    %% A simulation ends for clients when all clients have
    %% observed all ads disabled (first component of the map in
    %% the simulation status instance is true for all clients)
    {ok, AdsDisabledAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithAdsDisabled = lists:filter(
        fun({_Node, {AdsDisabled, _LogsPushed}}) ->
            AdsDisabled
        end,
        AdsDisabledAndLogs
    ),

    case length(NodesWithAdsDisabled) == client_number() of
        true ->
            lager:info("All nodes observed ads disabled. Node ~p", [node()]),
            lasp:update(?SIM_STATUS_ID, {Actor, {snd, true}}, Actor),
            lasp_simulation_support:push_logs();
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
schedule_impression() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?IMPRESSION_INTERVAL),
    erlang:send_after(?IMPRESSION_INTERVAL + Jitter, self(), view).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).

%% @private
schedule_check_simulation_end() ->
    erlang:send_after(?STATUS_INTERVAL, self(), check_simulation_end).

%% @private
client_number() ->
    lasp_config:get(client_number, 3).
