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

-module(lasp_advertisement_counter_server).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         trigger/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {actor, ads}).

-record(ad, {id, image, counter}).

-record(contract, {id}).

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

    %% Build DAG.
    {ok, Ads, AdList} = build_dag(),

    %% Initialize triggers.
    launch_triggers(AdList, Ads, Actor),

    %% Create instance for convergence tracking
    {Id, Type} = ?CONVERGENCE_ID,
    {ok, _} = lasp:declare(Id, Type),

    %% schedule check convergence or check push logs
    case lasp_simulation_support:should_push_logs() of
        true ->
            schedule_check_push_logs();
        _ ->
            schedule_check_convergence()
    end,

    {ok, #state{actor=Actor, ads=Ads}}.

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
handle_info(log, #state{ads=Ads}=State) ->
    %% Print number of enabled ads.
    {ok, AdList} = lasp:query(Ads),
    Size = sets:size(AdList),

    lager:info("Enabled advertisements: ~p", [Size]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(check_convergence, #state{}=State) ->
    {ok, {_, ConvergenceAndLogs}} = lasp:query(?CONVERGENCE_ID),

    NodesWithAllEvents = lists:filter(
        fun({_Node, {AllEvents, _LogsPushed}}) ->
            AllEvents
        end,
        ConvergenceAndLogs
    ),

    case length(NodesWithAllEvents) == client_number() of
        true ->
            lager:info("Convergence reached on all clients"),
            lasp_config:set(convergence, true),
            lasp_transmission_instrumentation:convergence(),
            init:stop();
        false ->
            schedule_check_convergence()
    end,

    {noreply, State};

handle_info(check_push_logs, #state{}=State) ->
    {ok, {_, ConvergenceAndLogs}} = lasp:query(?CONVERGENCE_ID),

    NodesWithLogsPushed = lists:filter(
        fun({_Node, {_AllEvents, LogsPushed}}) ->
            LogsPushed
        end,
        ConvergenceAndLogs
    ),

    case length(NodesWithLogsPushed) == client_number() of
        true ->
            lager:info("Logs pushed on all clients"),
            lasp_simulation_support:push_logs(),
            lasp_config:set(convergence, true),
            init:stop();
        false ->
            schedule_check_push_logs()
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
create_ads_and_contracts(Ads, Contracts) ->
    AdIds = lists:map(fun(_) ->
                              {ok, Unique} = lasp_unique:unique(),
                              Unique
                      end, lists:seq(1, ?ADS)),
    lists:map(fun(Id) ->
                {ok, _} = lasp:update(Contracts,
                                      {add, #contract{id=Id}},
                                      node())
                end, AdIds),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, {CounterId, _, _, _}} = lasp:declare(?COUNTER_TYPE),

                Ad = #ad{id=Id, counter=CounterId},

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(Ads, {add, Ad}, node()),

                Ad

                end, AdIds).

%% @private
build_dag() ->
    %% For each identifier, generate a contract.
    {ok, {Contracts, _, _, _}} = lasp:declare(?SET_TYPE),

    %% Generate Rovio's advertisements.
    {ok, {RovioAds, _, _, _}} = lasp:declare(?SET_TYPE),
    RovioAdList = create_ads_and_contracts(RovioAds, Contracts),

    %% Generate Riot's advertisements.
    {ok, {RiotAds, _, _, _}} = lasp:declare(?SET_TYPE),
    RiotAdList = create_ads_and_contracts(RiotAds, Contracts),

    %% Gather ads.
    AdList = RovioAdList ++ RiotAdList,

    %% Union ads.
    {ok, {Ads, _, _, _}} = lasp:declare(?SET_TYPE),
    ok = lasp:union(RovioAds, RiotAds, Ads),

    %% Compute the Cartesian product of both ads and contracts.
    {ok, {AdsContracts, _, _, _}} = lasp:declare(?SET_TYPE),
    ok = lasp:product(Ads, Contracts, AdsContracts),

    %% Filter items by join on item it.
    {ok, {AdsWithContracts, _, _, _}} = lasp:declare(?ADS_WITH_CONTRACTS, ?SET_TYPE),
    FilterFun = fun({#ad{id=Id1}, #contract{id=Id2}}) ->
        Id1 =:= Id2
    end,
    ok = lasp:filter(AdsContracts, FilterFun, AdsWithContracts),

    {ok, Ads, AdList}.

%% @private
launch_triggers(AdList, Ads, Actor) ->
    lists:map(fun(Ad) ->
                      spawn_link(fun() ->
                                         trigger(Ad, Ads, Actor)
                                 end)
              end, AdList).

%% @private
trigger(#ad{counter=CounterId} = Ad, Ads, Actor) ->
    %% Blocking threshold read for max advertisement impressions.
    {ok, Value} = lasp:read(CounterId, {value, ?MAX_IMPRESSIONS}),

    lager:info("Threshold for ~p reached; disabling!", [Ad]),
    lager:info("Counter: ~p", [Value]),

    %% Remove the advertisement.
    {ok, _} = lasp:update(Ads, {rmv, Ad}, Actor),

    ok.

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
schedule_check_convergence() ->
    erlang:send_after(?CONVERGENCE_INTERVAL, self(), check_convergence).

%% @private
schedule_check_push_logs() ->
    erlang:send_after(?CONVERGENCE_INTERVAL, self(), check_push_logs).

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).
