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

-module(lasp_group_by_stress_server).

-author("Junghun Yoo <junghun.yoo@cs.ox.ac.uk>").

-behaviour(gen_server).

%% API
-export([start_link/0]).
%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("lasp.hrl").
-include("lasp_group_by_stress.hrl").

%% State record.
-record(state, {actor, expected_result, completed_clients}).

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
    lager:info("Group-by stress test server initialized."),

    %% Delay for graph connectedness.
    wait_for_connectedness(),
    lasp_instrumentation:experiment_started(),

    %% Track whether simulation has ended or not.
    lasp_config:set(simulation_end, false),

    %% Generate actor identifier.
    Actor = node(),

    %% Schedule logging.
    schedule_logging(),

    %% Calculate the expected result.
    {ok, ExpectedResult} = calc_expected_result(),

    build_dag(),

    %% Create instance for clients completion tracking
    {Id, Type} = ?COUNTER_COMPLETED_CLIENTS,
    {ok, {CompletedClients, _, _, _}} = lasp:declare(Id, Type),

    %% Schedule check simulation end
    schedule_check_simulation_end(),

    {ok,
        #state{
            actor=Actor,
            expected_result=ExpectedResult,
            completed_clients=CompletedClients}}.

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
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Print number of docs.
    {ok, SetInput} = lasp:query(?SET_INPUT),
    lager:info("Number of Docs: ~p", [sets:size(SetInput)]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(
    check_simulation_end,
    #state{
        expected_result=ExpectedResult,
        completed_clients=CompletedClients}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for the server when the result CRDT is same as the
    %% expected result.
    {ok, GroupBySetInput} = lasp:query(?GROUP_BY_SET_INPUT),

    lager:info(
        "Checking for simulation end: current result: ~p.",
        [GroupBySetInput]),

    {ok, CounterCompletedClients} = lasp:query(CompletedClients),

    lager:info(
        "Checking for simulation end: completed clients: ~p.",
        [CounterCompletedClients]),

    CurGroupBySetInput =
        sets:fold(
            fun({ClientNum, SndSet}, AccCurGroupBySetInput) ->
                ordsets:add_element({ClientNum, SndSet}, AccCurGroupBySetInput)
            end,
            ordsets:new(),
            GroupBySetInput),
    case CurGroupBySetInput == ExpectedResult andalso
        CounterCompletedClients == client_number() of
        true ->
            lager:info("The result CRDT has the expected value."),
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
calc_expected_result() ->
    MaxImpressions = lasp_config:get(max_impressions, ?MAX_IMPRESSIONS_DEFAULT),
    Result =
        lists:foldl(
            fun(ClientNum, AccResult)->
                ordsets:add_element(
                    {ClientNum,
                        ordsets:from_list(lists:seq(1, MaxImpressions, 3))},
                    AccResult)
            end,
            ordsets:new(),
            lists:seq(1, client_number())),
    lager:info("Expected result: ~p", [Result]),
    {ok, Result}.

%% @private
build_dag() ->
    {IdInput, TypeInput} = ?SET_INPUT,
    {ok, {SetInput, _, _, _}} = lasp:declare(IdInput, TypeInput),

    {IdResult, TypeResult} = ?GROUP_BY_SET_INPUT,
    {ok, {GroupBySetInput, _, _, _}} = lasp:declare(IdResult, TypeResult),

    ok = lasp:group_by_first(SetInput, GroupBySetInput),

    ok.

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
stop_simulation() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            case sprinter:orchestration() of
                {ok, kubernetes} ->
                    lasp_kubernetes_simulations:stop();
                {ok, mesos} ->
                    lasp_marathon_simulations:stop()
            end
    end.

%% @private
wait_for_connectedness() ->
    case sprinter:orchestrated() of
        false ->
            ok;
        _ ->
            case sprinter_backend:was_connected() of
                {ok, true} ->
                    ok;
                {ok, false} ->
                    timer:sleep(100),
                    wait_for_connectedness()
            end
    end.
