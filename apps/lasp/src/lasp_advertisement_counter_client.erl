%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
-export([start_link/6,
         start/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("lasp.hrl").

%% State record.
-record(state, {set_type,
                counter_type,
                sync_interval,
                runner,
                id,
                ads_with_contracts_id,
                ads_with_contracts,
                counters,
                counters_delta,
                buffered_ops}).

-define(VIEW, 10).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(crdt(), crdt(), non_neg_integer(), pid(), id(), id()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(SetType, CounterType, SyncInterval, Runner, Id, AdsWithContractsId) ->
    gen_server:start_link(?MODULE, [SetType,
                                    CounterType,
                                    SyncInterval,
                                    Runner,
                                    Id,
                                    AdsWithContractsId], []).

%% @doc Start and *don't* link to calling process.
-spec start(crdt(), crdt(), non_neg_integer(), pid(), id(), id()) ->
    {ok, pid()} | ignore | {error, term()}.
start(SetType, CounterType, SyncInterval, Runner, Id, AdsWithContractsId) ->
    gen_server:start(?MODULE, [SetType,
                               CounterType,
                               SyncInterval,
                               Runner,
                               Id,
                               AdsWithContractsId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([SetType, CounterType, SyncInterval, Runner, Id, AdsWithContractsId]) ->
    %% Seed the random number generator.
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),

    %% Schedule first synchronization interval.
    erlang:send_after(SyncInterval, self(), sync),
    erlang:send_after(?VIEW, self(), view),

    {ok, #state{set_type=SetType,
                counter_type=CounterType,
                sync_interval=SyncInterval,
                runner=Runner,
                id=Id,
                ads_with_contracts_id=AdsWithContractsId,
                ads_with_contracts=undefined,
                counters=dict:new(),
                counters_delta=dict:new(),
                buffered_ops=0}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(terminate, _From, #state{set_type=SetType,
                                     runner=Runner,
                                     ads_with_contracts_id=AdsWithContractsId,
                                     ads_with_contracts=AdsWithContracts0,
                                     counters=Counters0,
                                     counters_delta=CountersDelta0,
                                     buffered_ops=BufferedOps}=State) ->
    %% Log flushed events before termination, if necessary.
    case BufferedOps of
        0 ->
            ok;
        _ ->
            lasp_advertisement_counter:synchronize(SetType,
                                                   AdsWithContractsId,
                                                   AdsWithContracts0,
                                                   Counters0,
                                                   CountersDelta0),
            lasp_advertisement_counter:log_divergence(flush,
                                                      BufferedOps)
    end,

    %% Respond that the termination is done.
    Runner ! terminate_done,

    {reply, ok, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(view, #state{counter_type=CounterType,
                         id=Id,
                         runner=Runner,
                         counters=Counters0,
                         counters_delta=CountersDelta0,
                         buffered_ops=BufferedOps0}=State) ->
    %% Reschedule next advertisement view.
    erlang:send_after(?VIEW, self(), view),

    {ok, {Counters,
          CountersDelta}} = lasp_advertisement_counter:view_ad(CounterType,
                                                               Id,
                                                               Counters0,
                                                               CountersDelta0),

    %% Notify the harness that an event has been processed.
    Runner ! view_ad_complete,

    %% Log buffered event.
    lasp_advertisement_counter:log_divergence(buffer, 1),

    {noreply, State#state{counters=Counters,
                          counters_delta=CountersDelta,
                          buffered_ops=BufferedOps0 + 1}};
handle_info(sync, #state{set_type=SetType,
                         sync_interval=SyncInterval,
                         ads_with_contracts_id=AdsWithContractsId,
                         ads_with_contracts=AdsWithContracts0,
                         counters=Counters0,
                         counters_delta=CountersDelta0,
                         buffered_ops=BufferedOps0}=State) ->
    {ok, AdsWithContracts,
         Counters} = lasp_advertisement_counter:synchronize(SetType,
                                                            AdsWithContractsId,
                                                            AdsWithContracts0,
                                                            Counters0,
                                                            CountersDelta0),
    %% Log flushed events.
    case BufferedOps0 of
        0 ->
            ok;
        _ ->
            lasp_advertisement_counter:log_divergence(flush, BufferedOps0)
    end,
    BufferedOps = 0,

    %% Flush deltas.
    CountersDelta = dict:new(),

    %% Reschedule sychronization.
    Jitter = random:uniform(SyncInterval),
    NextSyncInterval = SyncInterval + Jitter,
    erlang:send_after(NextSyncInterval, self(), sync),

    {noreply, State#state{ads_with_contracts=AdsWithContracts,
                          counters=Counters,
                          counters_delta=CountersDelta,
                          buffered_ops=BufferedOps}};
handle_info(terminate, #state{set_type=SetType,
                              runner=Runner,
                              ads_with_contracts_id=AdsWithContractsId,
                              ads_with_contracts=AdsWithContracts0,
                              counters=Counters0,
                              counters_delta=CountersDelta0,
                              buffered_ops=BufferedOps}=State) ->
    %% Log flushed events before termination, if necessary.
    case BufferedOps of
        0 ->
            ok;
        _ ->
            lasp_advertisement_counter:synchronize(SetType,
                                                   AdsWithContractsId,
                                                   AdsWithContracts0,
                                                   Counters0,
                                                   CountersDelta0),
            lasp_advertisement_counter:log_divergence(flush,
                                                      BufferedOps)
    end,

    %% Respond that the termination is done.
    Runner ! terminate_done,

    {stop, normal, State};
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
