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
-define(IMPRESSION_INTERVAL, 1000).
-define(LOG_INTERVAL, 10000).
-define(CONVERGENCE_INTERVAL, 5000).

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

    %% Schedule check convergence
    schedule_check_convergence(),

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
            {ok, _} = lasp:update(convergence_id(), {fst, increment}, Actor),

            %% Increment impressions.
            Impressions0 + 1
    end,

    %% Schedule advertisement counter impression.
    case Impressions1 < max_impressions() of
        true ->
            schedule_impression();
        false ->
            lager:info("Max number of impressions reached. Node: ~p", [node()])
    end,

    {noreply, State#state{impressions=Impressions1}};

handle_info(check_convergence, #state{actor=Actor}=State) ->
    MaxEvents = max_impressions() * client_number(),
    {ok, {TotalEvents, C}} = lasp:query(convergence_id()),
    lager:info("Total number of events observed so far ~p of ~p", [TotalEvents, MaxEvents]),

    case TotalEvents == MaxEvents of
        true ->
            lager:info("Convergence reached on node ~p", [node()]),
            lasp:update(convergence_id(), {snd, {Actor, true}}, Actor);
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
schedule_impression() ->
    %% Add random jitter.
    Jitter = random:uniform(?IMPRESSION_INTERVAL),
    erlang:send_after(?IMPRESSION_INTERVAL + Jitter, self(), view).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).

%% @private
schedule_check_convergence() ->
    erlang:send_after(?CONVERGENCE_INTERVAL, self(), check_convergence).

%% @private
max_impressions() ->
    lasp_config:get(simulation_event_number, 10).

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
