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

%% @todo I don't believe the cache is even necessary, but use it for
%%       now.  At this point, it's superfluous, given that it
%%       immediately updates the cache on update.

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

%% State record.
-record(state, {actor, cache, impressions}).

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

    %% Delay initialization to ensure the application has been declared.
    %%
    %% @todo Main issue here is that the declare-if-not-found behaviour
    %% doesn't store anything in waiting-threads or store the pending
    %% threshold read, which causes this to halt forever.
    %%
    timer:sleep(5000),

    %% Schedule initial state transfer for counters.
    refresh_advertisement_counters(undefined),

    %% Schedule advertisement counter impression.
    schedule_impression(),

    %% Schedule logging.
    schedule_logging(),

    %% Start local cache out at zero.
    Cache = dict:new(),

    {ok, #state{actor=Actor, impressions=0, cache=Cache}}.

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
    lager:info("Impressions: ~p", [Impressions]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};
handle_info(view, #state{actor=Actor, impressions=Impressions, cache=Cache}=State0) ->
    %% Find list of ads in the cache.
    State = case dict:find({?ADS_WITH_CONTRACTS, ?SET_TYPE}, Cache) of
        {ok, {_Id, Type, _Metadata, Value}} ->
            %% Get value.
            Ads = lasp_type:query(Type, Value),

            %% Select random.
            Random = lasp_support:puniform(sets:size(Ads)),

            %% @todo Exposes internal details of record.
            {{ad, _, _, Counter},
             _Contract} = lists:nth(Random, sets:to_list(Ads)),

            %% Increment counter.
            {ok, _} = lasp:update(Counter, increment, Actor),

            %% Increment impressions.
            State0#state{impressions=Impressions+1};
        error ->
            exit(no_ads)
    end,

    %% Schedule advertisement counter impression.
    schedule_impression(),

    {noreply, State};
handle_info({Id, _Type, _Metadata, Value}=Result,
            #state{cache=Cache0}=State) ->
    %% Update cache.
    Cache = dict:store(Id, Result, Cache0),

    %% Schedule initial state transfer for counters.
    refresh_advertisement_counters(Value),

    {noreply, State#state{cache=Cache}};
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
refresh_advertisement_counters(Previous) ->
    Self = self(),
    spawn_link(fun() ->
                       %% Wait for updated counters.
                       {ok, Result} = lasp:read({?ADS_WITH_CONTRACTS, ?SET_TYPE},
                                                {strict, Previous}),

                       %% Send them back to the gen_server.
                       Self ! Result
               end).

%% @private
schedule_impression() ->
    erlang:send_after(?IMPRESSION_INTERVAL, self(), view).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).
