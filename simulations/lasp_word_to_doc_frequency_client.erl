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

-module(lasp_word_to_doc_frequency_client).

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
-include("lasp_word_to_doc_frequency.hrl").

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
    lager:info("Word to doc frequency calculator client initialized."),

    %% Generate actor identifier.
    Actor = node(),

    Node = atom_to_list(Actor),
    {ClientNum, _Rest} =
        string:to_integer(
            string:substr(Node, length("client_") + 1, length(Node))),

    lager:info("ClientNum: ~p", [ClientNum]),

    {IdInput, TypeInput} = ?SET_DOC_ID_TO_CONTENTS,
    {ok, {SetDocIdToContents, _, _, _}} = lasp:declare(IdInput, TypeInput),

    %% Schedule input CRDT updates.
    schedule_input_update(),

    %% Build DAG.
    case lasp_config:get(heavy_client, false) of
        true ->
            build_dag(SetDocIdToContents);
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
            input_id=SetDocIdToContents,
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
    {ok, SetDocIdToContents} = lasp:query(?SET_DOC_ID_TO_CONTENTS),
    {ok, SetWordToDocFrequency} = lasp:query(?SET_WORD_TO_DOC_FREQUENCY),
    lager:info(
        "InputUpdates: ~p, current number of docs: ~p, node: ~p, result: ~p",
        [InputUpdates,
            sets:size(SetDocIdToContents),
            Actor,
            SetWordToDocFrequency]),

    %% Schedule logging.
    schedule_logging(),

    {noreply, State};

handle_info(
    input_update,
    #state{
        actor=Actor,
        client_num=ClientNum,
        input_updates=InputUpdates0,
        input_id=SetDocIdToContents}=State) ->
    lasp_marathon_simulations:log_message_queue_size("input_update"),

    InputUpdates1 = InputUpdates0 + 1,

    lager:info("Current update: ~p, node: ~p", [InputUpdates1, Actor]),

    case InputUpdates1 rem 3 == 0 of
        false ->
            CurInput =
                lists:nth(
                    InputUpdates1 - (InputUpdates1 div 3),
                    lists:nth(ClientNum, ?INPUT_DATA)),
            {ok, _} = lasp:update(SetDocIdToContents, {add, CurInput}, Actor),
            {ok, Value} = lasp:query(SetDocIdToContents),
            lager:info(
                "Current input: ~p, Current docs: ~p",
                [CurInput, sets:size(Value)]);
        true ->
            RemovingInput =
                lists:nth(
                    InputUpdates1 - (InputUpdates1 div 3),
                    lists:nth(ClientNum, ?INPUT_DATA)),
            {ok, _} =
                lasp:update(SetDocIdToContents, {rmv, RemovingInput}, Actor),
            {ok, Value} = lasp:query(SetDocIdToContents),
            lager:info(
                "Current removing input: ~p, Current docs: ~p",
                [RemovingInput, sets:size(Value)])
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
    {ok, WordToDocFrequency} = lasp:query(?SET_WORD_TO_DOC_FREQUENCY),

    lager:info(
        "Checking for simulation end: current result: ~p.",
        [WordToDocFrequency]),

    {ok, CounterCompletedClients} = lasp:query(CompletedClients),

    lager:info(
        "Checking for simulation end: completed clients: ~p.",
        [CounterCompletedClients]),

    case WordToDocFrequency == ExpectedResult of
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
calc_expected_result() ->
    DictDocIdToContents =
        lists:foldl(
            fun(ListInput, AccDictDocIdToContents0) ->
                {AccDictDocIdToContents, _} =
                    lists:foldl(
                        fun({DocId, Contents},
                            {AccDictDocIdToContents1, AccIndex}) ->
                            NewAccDictDocIdToContents1 =
                                case AccIndex rem 2 == 1 of
                                    true ->
                                        orddict:store(
                                            DocId,
                                            Contents,
                                            AccDictDocIdToContents1);
                                    false ->
                                        AccDictDocIdToContents1
                                end,
                            {NewAccDictDocIdToContents1, AccIndex + 1}
                        end,
                        {AccDictDocIdToContents0, 1},
                        ListInput),
                AccDictDocIdToContents
            end,
            orddict:new(),
            ?INPUT_DATA),
    DictDocIdToWords =
        orddict:map(
            fun(_DocId, Contents) ->
                Words = string:tokens(Contents, " "),
                WordsNoDup = sets:from_list(Words),
                WordsNoDup
            end,
            DictDocIdToContents),
    DictWordToDocCount =
        orddict:fold(
            fun(_DocId, WordsNoDup, AccDictWordToDocCount0) ->
                sets:fold(
                    fun(Word, AccDictWordToDocCount1) ->
                        orddict:update_counter(Word, 1, AccDictWordToDocCount1)
                    end,
                    AccDictWordToDocCount0,
                    WordsNoDup)
            end,
            orddict:new(),
            DictDocIdToWords),
    DictWordToDocFrequency =
        orddict:map(
            fun(_Word, DocCount) ->
                DocCount / orddict:size(DictDocIdToContents)
            end,
            DictWordToDocCount),
    SetWordToDocFrequency =
        orddict:fold(
            fun(Word, DocFrequency, AccSetWordToDocFrequency) ->
                sets:add_element({Word, DocFrequency}, AccSetWordToDocFrequency)
            end,
            sets:new(),
            DictWordToDocFrequency),
    lager:info("Expected result: ~p", [SetWordToDocFrequency]),
    {ok, SetWordToDocFrequency}.

%% @private
build_dag(SetDocIdToContents) ->
    {Id1, Type1} = ?SET_DOC_ID_TO_WORDS_NO_DUP,
    {ok, {SetDocIdToWordsNoDup, _, _, _}} = lasp:declare(Id1, Type1),

    ok =
        lasp:map(
            SetDocIdToContents,
            fun({DocId, Contents}) ->
                Words = string:tokens(Contents, " "),
                WordsNoDup = sets:from_list(Words),
                WordAndDocIds =
                    sets:fold(
                        fun(Word, AccWordAndDocIds) ->
                            lists:append(AccWordAndDocIds, [{Word, DocId}])
                        end,
                        [],
                        WordsNoDup),
                WordAndDocIds
            end,
            SetDocIdToWordsNoDup),

    {Id2, Type2} = ?SET_WORD_TO_DOC_IDS,
    {ok, {SetWordToDocIds, _, _, _}} = lasp:declare(Id2, Type2),

    ok = lasp:singleton(SetDocIdToWordsNoDup, SetWordToDocIds),

    {Id3, Type3} = ?SET_WORD_TO_DOC_COUNTS,
    {ok, {SetWordToDocCounts, _, _, _}} = lasp:declare(Id3, Type3),

    ok =
        lasp:map(
            SetWordToDocIds,
            fun(ListWordAndDocIds) ->
                WordToDocCounts =
                    lists:foldl(
                        fun({Word,_DocId}, AccWordToDocCounts) ->
                            orddict:update_counter(Word, 1, AccWordToDocCounts)
                        end,
                        orddict:new(),
                        ListWordAndDocIds),
                orddict:to_list(WordToDocCounts)
            end,
            SetWordToDocCounts),

    {Id4, Type4} = ?SET_WORD_TO_DOC_COUNT_LIST,
    {ok, {SetWordToDocCountList, _, _, _}} = lasp:declare(Id4, Type4),

    ok = lasp:unsingleton(SetWordToDocCounts, SetWordToDocCountList),

    {Id5, Type5} = ?SET_WORD_TO_DOC_COUNT,
    {ok, {SetWordToDocCount, _, _, _}} = lasp:declare(Id5, Type5),

    ok =
        lasp:map(
            SetWordToDocCountList,
            fun([WordToDocCount]) ->
                WordToDocCount
            end,
            SetWordToDocCount),

    {Id6, Type6} = ?SIZE_T_DOC_ID_TO_CONTENTS,
    {ok, {SizeTDocIdToContents, _, _, _}} = lasp:declare(Id6, Type6),

    ok = lasp:length(SetDocIdToContents, SizeTDocIdToContents),

    {Id7, Type7} = ?SET_WORD_TO_DOC_COUNT_AND_NUM_OF_DOCS,
    {ok, {SetWordToDocCountAndNumOfDocs, _, _, _}} = lasp:declare(Id7, Type7),

    ok =
        lasp:product(
            SetWordToDocCount,
            SizeTDocIdToContents,
            SetWordToDocCountAndNumOfDocs),

    {IdResult, TypeResult} = ?SET_WORD_TO_DOC_FREQUENCY,
    {ok, {SetWordToDocFrequency, _, _, _}} = lasp:declare(IdResult, TypeResult),

    ok =
        lasp:map(
            SetWordToDocCountAndNumOfDocs,
            fun({{Word, DocCount}, NumOfDocsSizeT}) ->
                NumOfDocs = NumOfDocsSizeT,
                {Word, DocCount / NumOfDocs}
            end,
            SetWordToDocFrequency),

    ok.
