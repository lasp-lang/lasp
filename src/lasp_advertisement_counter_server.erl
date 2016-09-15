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
    {ok, AdList} = build_dag(Actor),

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
    log_message_queue_size("log"),

    %% Print number of enabled ads.
    {ok, Ads} = lasp:query(?ADS),

    lager:info("Enabled advertisements: ~p", [sets:size(Ads)]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(check_simulation_end, #state{adlist=AdList}=State) ->
    log_message_queue_size("check_simulation_end"),

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

    NodesWithAdsDisabled = lists:filter(
        fun({_Node, {AdsDisabled, _LogsPushed}}) ->
            AdsDisabled
        end,
        AdsDisabledAndLogs
    ),

    lager:info("Checking for simulation end: ~p nodes with ads disabled and ~p nodes with logs pushed.",
               [length(NodesWithAdsDisabled), length(NodesWithLogsPushed)]),

    case length(NodesWithLogsPushed) == client_number() of
        true ->
            lager:info("All nodes have pushed their logs"),
            log_overcounting_and_convergence(AdList),
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
create_ads_and_contracts(Ads, Contracts, Actor) ->
    AdIds = lists:map(fun(_) ->
                              {ok, Unique} = lasp_unique:unique(),
                              Unique
                      end, lists:seq(1, ?ADS_NUMBER)),
    lists:map(fun(Id) ->
                {ok, _} = lasp:update(Contracts,
                                      {add, #contract{id=Id}},
                                      Actor)
                end, AdIds),
    lists:map(fun(Id) ->
                %% Generate a G-Counter.
                {ok, {CounterId, _, _, _}} = lasp:declare(?COUNTER_TYPE),

                Ad = #ad{id=Id, name=Id, counter=CounterId},

                %% Add it to the advertisement set.
                {ok, _} = lasp:update(Ads, {add, Ad}, Actor),

                Ad

                end, AdIds).

%% @private
build_dag(Actor) ->
    %% For each identifier, generate a contract.
    {ContractsId, ContractsType} = ?CONTRACTS,
    {ok, {Contracts, _, _, _}} = lasp:declare(ContractsId, ContractsType),

    %% Generate Rovio's advertisements.
    {ok, {RovioAds, _, _, _}} = lasp:declare(?SET_TYPE),
    RovioAdList = create_ads_and_contracts(RovioAds, Contracts, Actor),

    %% Generate Riot's advertisements.
    {ok, {RiotAds, _, _, _}} = lasp:declare(?SET_TYPE),
    RiotAdList = create_ads_and_contracts(RiotAds, Contracts, Actor),

    %% Gather ads.
    AdList = RovioAdList ++ RiotAdList,

    %% Union ads.
    {AdsId, AdsType} = ?ADS,
    {ok, _} = lasp:declare(AdsId, AdsType),
    ok = lasp:union(RovioAds, RiotAds, ?ADS),

    %% Compute the Cartesian product of both ads and contracts.
    {AdsContractsId, AdsContractsType} = ?ADS_CONTRACTS,
    {ok, _} = lasp:declare(AdsContractsId, AdsContractsType),
    ok = lasp:product(?ADS, ?CONTRACTS, ?ADS_CONTRACTS),

    %% Filter items by join on item it.
    {AdsWithContractsId, AdsWithContractsType} = ?ADS_WITH_CONTRACTS,
    {ok, _} = lasp:declare(AdsWithContractsId, AdsWithContractsType),
    FilterFun = fun({#ad{id=Id1}, #contract{id=Id2}}) ->
        Id1 =:= Id2
    end,
    ok = lasp:filter(?ADS_CONTRACTS, FilterFun, ?ADS_WITH_CONTRACTS),

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
    MaxImpressions = lasp_config:get(max_impressions,
                                     ?MAX_IMPRESSIONS_DEFAULT),

    EnforceFun = fun() ->
            lager:info("Threshold for ~p reached; disabling!", [Ad]),

            %% Remove the advertisement.
            {ok, _} = lasp:update(?ADS, {rmv, Ad}, Actor)
    end,
    lasp:invariant(CounterId, {value, MaxImpressions}, EnforceFun),

    ok.

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
log_overcounting_and_convergence(AdList) ->
    case lasp_config:get(instrumentation, false) of
        true ->
            Overcounting = compute_overcounting(AdList),
            lasp_instrumentation:overcounting(Overcounting),
            lasp_instrumentation:convergence();

        false ->
            ok
    end.

%% @private
compute_overcounting(AdList) ->
    OvercountingSum = lists:foldl(
        fun(#ad{counter=CounterId} = _Ad, Acc) ->
            {ok, Value} = lasp:query(CounterId),
            MaxImpressions = lasp_config:get(max_impressions,
                                             ?MAX_IMPRESSIONS_DEFAULT),
            Overcounting = Value - MaxImpressions,
            OvercountingPercentage = (Overcounting * 100) / MaxImpressions,
            Acc + OvercountingPercentage
        end,
        0,
        AdList
    ),

    OvercountingSum / length(AdList).

%% @private
stop_simulation() ->
    DCOS = os:getenv("DCOS", "false"),

    case list_to_atom(DCOS) of
        false ->
            ok;
        _ ->
            Token = os:getenv("TOKEN", "undefined"),
            EvalTimestamp = lasp_config:get(evaluation_timestamp, 0),
            RunningApps = [
                "lasp-client-" ++ integer_to_list(EvalTimestamp),
                "lasp-server-" ++ integer_to_list(EvalTimestamp)
            ],

            lists:foreach(
                fun(AppName) ->
                    delete_marathon_app(DCOS, Token, AppName)
                end,
                RunningApps
            )
    end.

%% @private
delete_marathon_app(DCOS, Token, AppName) ->
    Headers = [{"Authorization", "token=" ++ Token}],
    Url = DCOS ++ "/marathon/v2/apps/" ++ AppName,
    case httpc:request(delete, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, _Body}} ->
            ok;
        Other ->
            lager:info("Delete app ~p request failed: ~p", [AppName, Other])
    end.

%% @private
wait_for_connectedness() ->
    case os:getenv("DCOS", "false") of
        "false" ->
            ok;
        _ ->
            case sprinter:was_connected() of
                {ok, true} ->
                    ok;
                {ok, false} ->
                    timer:sleep(100),
                    wait_for_connectedness()
            end
    end.

%% @private
log_message_queue_size(Method) ->
    {message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    lasp_logger:mailbox("MAILBOX " ++ Method ++ " message processed; messages remaining: ~p", [MessageQueueLen]).

