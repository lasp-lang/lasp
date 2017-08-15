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

-module(lasp_group_by_stress_client).

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
-record(
    state,
    {actor,
        client_num,
        input_updates,
        input_id,
        expected_result,
        completed_clients}).

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
    lager:info("Group-by stress test client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    Node = atom_to_list(Actor),
    {ClientNum, _Rest} =
        string:to_integer(
            string:substr(Node, length("client_") + 1, length(Node))),

    lager:info("ClientNum: ~p", [ClientNum]),

    {IdInput, TypeInput} = ?SET_INPUT,
    {ok, {SetInput, _, _, _}} = lasp:declare(IdInput, TypeInput),

    %% Schedule input CRDT updates.
    schedule_input_update(),

    %% Build DAG.
    case lasp_config:get(heavy_client, false) of
        true ->
            build_dag(SetInput);
        false ->
            ok
    end,

    %% Schedule logging.
    schedule_logging(),

    %% Calculate the expected result.
    {ok, ExpectedResult} = calc_expected_result(),

    %% Create instance for clients completion tracking
    {Id, Type} = ?COUNTER_COMPLETED_CLIENTS,
    {ok, {CompletedClients, _, _, _}} = lasp:declare(Id, Type),

    {ok,
        #state{
            actor=Actor,
            client_num=ClientNum,
            input_updates=0,
            input_id=SetInput,
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
handle_info(
    log, #state{actor=Actor, input_updates=InputUpdates}=State) ->
    lasp_marathon_simulations:log_message_queue_size("log"),

    %% Get current value of the input / output CRDTs.
    {ok, SetInput} = lasp:query(?SET_INPUT),
    {ok, GroupBySetInput} = lasp:query(?GROUP_BY_SET_INPUT),
    lager:info(
        "InputUpdates: ~p, current number of docs: ~p, node: ~p, result: ~p",
        [InputUpdates,
            sets:size(SetInput),
            Actor,
            GroupBySetInput]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(
    input_update,
    #state{
        actor=Actor,
        client_num=ClientNum,
        input_updates=InputUpdates0,
        input_id=SetInput}=State) ->
    lasp_marathon_simulations:log_message_queue_size("input_update"),

    InputUpdates1 = InputUpdates0 + 1,

    lager:info("Current update: ~p, node: ~p", [InputUpdates1, Actor]),

    case InputUpdates1 rem 3 == 0 of
        false ->
            {ok, _} =
                lasp:update(SetInput, {add, {ClientNum, InputUpdates1}}, Actor),
            {ok, Value} = lasp:query(SetInput),
            lager:info(
                "Current input: ~p, Current docs: ~p",
                [{ClientNum, InputUpdates1}, sets:size(Value)]);
        true ->
            {ok, _} =
                lasp:update(
                    SetInput, {rmv, {ClientNum, InputUpdates1 - 1}}, Actor),
            {ok, Value} = lasp:query(SetInput),
            lager:info(
                "Current removing input: ~p, Current docs: ~p",
                [{ClientNum, InputUpdates1 - 1}, sets:size(Value)])
    end,

    MaxImpressions = lasp_config:get(max_impressions, ?MAX_IMPRESSIONS_DEFAULT),
    case InputUpdates1 == MaxImpressions of
        true ->
            lager:info("All updates are done. Node: ~p", [Actor]),
            log_convergence(),
            schedule_check_simulation_end();
        false ->
            schedule_input_update()
    end,

    {noreply, State#state{input_updates=InputUpdates1}};

handle_info(
    check_simulation_end,
    #state{
        actor=Actor,
        expected_result=ExpectedResult,
        completed_clients=CompletedClients}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for clients when the value of the result CRDT is
    %% same as the expected result.
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
    case CurGroupBySetInput == ExpectedResult of
        true ->
            lager:info("This node gets the expected result. Node ~p", [Actor]),
            lasp_instrumentation:stop(),
            lasp_support:push_logs(),
            lasp:update(CompletedClients, increment, Actor);
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
schedule_input_update() ->
    timer:send_after(?IMPRESSION_INTERVAL, input_update).

%% @private
schedule_logging() ->
    timer:send_after(?LOG_INTERVAL, log).

%% @private
schedule_check_simulation_end() ->
    timer:send_after(?STATUS_INTERVAL, check_simulation_end).

%% @private
log_convergence() ->
    case lasp_config:get(instrumentation, false) of
        true ->
            lasp_instrumentation:convergence();
        false ->
            ok
    end.

%% @private
client_number() ->
    lasp_config:get(client_number, 3).

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
build_dag(SetInput) ->
    {IdResult, TypeResult} = ?GROUP_BY_SET_INPUT,
    {ok, {GroupBySetInput, _, _, _}} = lasp:declare(IdResult, TypeResult),

    ok = lasp:group_by_first(SetInput, GroupBySetInput),

    ok.
