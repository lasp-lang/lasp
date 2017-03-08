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
-record(state, {actor, impressions, triggers}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([term()]) -> {ok, #state{}}.
init([]) ->
    lager:info("Advertisement counter client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule advertisement counter impression.
    schedule_impression(),

    %% Build DAG.
    case lasp_config:get(heavy_client, false) of
        true ->
            build_dag();
        false ->
            ok
    end,

    %% Schedule logging.
    schedule_logging(),

    {ok, #state{actor=Actor, impressions=0, triggers=dict:new()}}.

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
handle_info(log, #state{actor=Actor,
                        impressions=Impressions}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Get current value of the list of advertisements.
    {ok, Ads} = lasp:query(?ADS_WITH_CONTRACTS),

    lager:info("Impressions: ~p, current ad size: ~p, node: ~p", [Impressions, sets:size(Ads), Actor]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(view, #state{actor=Actor,
                         impressions=Impressions0,
                         triggers=Triggers0}=State) ->
    lasp_marathon_simulations:log_message_queue_size("view"),

    %% Get current value of the list of advertisements.
    {ok, Ads0} = lasp:query(?ADS_WITH_CONTRACTS),

    %% Make sure we have ads...
    {Impressions1, Triggers1} = case sets:size(Ads0) of
        0 ->
            %% Do nothing.
            {Impressions0, Triggers0};
        Size ->
            %% Select random.
            Random = lasp_support:puniform(Size),

            {#ad{counter=Counter, register=Register} = Ad, _Contract} =
                lists:nth(Random, sets:to_list(Ads0)),

            %% Spawn a process to disable the advertisement if it goes
            %% above the maximum number of impressions.
            %%
            Triggers = case lasp_config:get(heavy_client, false) of
                true ->
                    case dict:find(Ad, Triggers0) of
                        {ok, _Pid} ->
                            Triggers0;
                        error ->
                            Pid = spawn_link(fun() -> trigger(Ad, Actor) end),
                            dict:store(Ad, Pid, Triggers0)
                    end;
                false ->
                    Triggers0
            end,

            %% Increment counter.
            {ok, _} = lasp:update(Counter, increment, Actor),
            {ok, Value} = lasp:query(Counter),

            %% Increment register.
            {ok, _} = lasp:update(Register, increment, Actor),

            lager:info("Impressions seen: ~p, node: ~p", [Value, Actor]),

            %% Increment impressions.
            {Impressions0 + 1, Triggers}
    end,

    {ok, Ads} = lasp:query(?ADS_WITH_CONTRACTS),
    {ok, {_AdsId, AdsType, _AdsMetadata, AdsValue}} = lasp:read(?ADS_WITH_CONTRACTS, undefined),

    %% - If there are no ads (`sets:size(Ads) == 0')
    %%   it might mean the experiment hasn't started or it has started
    %%   and all ads are disabled.
    %%   If `not lasp_type:is_bottom(AdsType, AdsValue)' then the experiment
    %%   has started. With both, it means the experiement has ended
    %% - Else, keep doing impressions
    case sets:size(Ads) == 0 andalso not lasp_type:is_bottom(AdsType, AdsValue) of
        true ->
            lager:info("All ads are disabled. Node: ~p", [Actor]),

            %% Update Simulation Status Instance
            lasp:update(?SIM_STATUS_ID, {apply, Actor, {fst, true}}, Actor),
            log_convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_impression()
    end,

    {noreply, State#state{impressions=Impressions1, triggers=Triggers1}};

handle_info(check_simulation_end, #state{actor=Actor}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

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
            lager:info("All nodes observed ads disabled. Node ~p", [Actor]),
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
schedule_impression() ->
    timer:send_after(?IMPRESSION_INTERVAL, view).

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
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
build_dag() ->
    %% For each identifier, generate a contract.
    {ContractsId, ContractsType} = ?CONTRACTS,
    {ok, _} = lasp:declare(ContractsId, ContractsType),

    {AdsId, AdsType} = ?ADS,
    {ok, _} = lasp:declare(AdsId, AdsType),

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

    ok.

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
