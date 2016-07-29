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
-record(state, {actor, adlist}).

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

    %% Track whether simulation has ended or not.
    lasp_config:set(simulation_end, false),

    %% Generate actor identifier.
    Actor = self(),

    %% Schedule logging.
    schedule_logging(),

    %% Build DAG.
    {ok, AdList} = build_dag(),

    %% Initialize triggers.
    launch_triggers(AdList, Actor),

    %% Create instance for simulation status tracking
    {Id, Type} = ?SIM_STATUS_ID,
    {ok, _} = lasp:declare(Id, Type),

    %% Schedule check simulation end
    schedule_check_simulation_end(),

    {ok, #state{actor=Actor, adlist=AdList}}.

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
    %% Print number of enabled ads.
    {ok, Ads} = lasp:query({?ADS, ?SET_TYPE}),

    lager:info("Enabled advertisements: ~p", [sets:size(Ads)]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(check_simulation_end, #state{adlist=AdList}=State) ->
    %% A simulation ends for the server when all clients have
    %% observed that all clients observed all ads disabled and
    %% pushed their logs (second component of the map in
    %% the simulation status instance is true for all clients)
    {ok, AdsDisabledAndLogs} = lasp:query(?SIM_STATUS_ID),

    NodesWithLogsPushed = lists:filter(
        fun({_Node, {_AdsDisabled, LogsPushed}}) ->
            LogsPushed
        end,
        AdsDisabledAndLogs
    ),

    case length(NodesWithLogsPushed) == client_number() of
        true ->
            lager:info("All nodes have pushed their logs"),
            log_convergence(),
            lasp_support:push_logs(),
            log_divergence(AdList),
            lasp_config:set(simulation_end, true);
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
create_ads_and_contracts(Ads, Contracts) ->
    AdIds = lists:map(fun(_) ->
                              {ok, Unique} = lasp_unique:unique(),
                              Unique
                      end, lists:seq(1, ?ADS_NUMBER)),
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
    {ok, {Ads, _, _, _}} = lasp:declare(?ADS, ?SET_TYPE),
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

    {ok, AdList}.

%% @private
launch_triggers(AdList, Actor) ->
    lists:map(
        fun(Ad) ->
            spawn_link(
                fun() ->
                    trigger(Ad, Actor)
                end
            )
        end,
        AdList
    ).

%% @private
trigger(#ad{counter=CounterId} = Ad, Actor) ->
    %% Blocking threshold read for max advertisement impressions.
    {ok, Value} = lasp:read(CounterId, {value, ?MAX_IMPRESSIONS}),

    lager:info("Threshold for ~p reached; disabling!", [Ad]),
    lager:info("Counter: ~p", [Value]),

    %% Remove the advertisement.
    {ok, _} = lasp:update({?ADS, ?SET_TYPE}, {rmv, Ad}, Actor),

    ok.

%% @private
schedule_logging() ->
    erlang:send_after(?LOG_INTERVAL, self(), log).

%% @private
schedule_check_simulation_end() ->
    erlang:send_after(?STATUS_INTERVAL, self(), check_simulation_end).

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_transmission_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
log_divergence(AdList) ->
    Filename = filename(),
    Divergence = compute_divergence(AdList),
    lager:info("Divergence ~p", [Divergence]),

    ok = file:write_file(
        Filename,
        io_lib:format("~w", [Divergence]),
        [write]
    ),
    ok.

%% @private
filename() ->
    EvalIdentifier = case lasp_config:get(evaluation_identifier, undefined) of
        undefined ->
            "undefined";
        {Simulation, Id} ->
            atom_to_list(Simulation) ++ "/" ++ atom_to_list(Id)
    end,
    EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
    Filename = "divergence",
    Dir = code:priv_dir(?APP) ++ "/evaluation/logs/"
        ++ EvalIdentifier ++ "/"
        ++ integer_to_list(EvalTimestamp) ++ "/",
    filelib:ensure_dir(Dir),
    Dir ++ Filename.

%% @private
compute_divergence(AdList) ->
    DivergenceSum = lists:foldl(
        fun(#ad{counter=CounterId} = _Ad, Acc) ->
            {ok, Value} = lasp:query(CounterId),
            Divergence = Value - ?MAX_IMPRESSIONS,
            Acc + Divergence
        end,
        0,
        AdList
    ),

    DivergenceSum / length(AdList).
