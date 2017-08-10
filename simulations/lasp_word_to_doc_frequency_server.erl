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

-module(lasp_word_to_doc_frequency_server).

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
-record(state, {actor, expected_result}).

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
    lager:info("Word to doc frequency calculator server initialized."),

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

    %% Create instances for the input and result CRDTs.
    {IdInput, TypeInput} = ?SET_DOC_ID_TO_CONTENTS,
    {ok, {SetDocIdToContents, _, _, _}} = lasp:declare(IdInput, TypeInput),
    {IdResult, TypeResult} = ?SET_WORD_TO_DOC_FREQUENCY,
    {ok, {SetWordToDocFrequency, _, _, _}} = lasp:declare(IdResult, TypeResult),

    build_dag(SetDocIdToContents, SetWordToDocFrequency),

    %% Create instance for clients completion tracking
    {Id, Type} = ?COUNTER_COMPLETED_CLIENTS,
    {ok, _} = lasp:declare(Id, Type),

    %% Schedule check simulation end
    schedule_check_simulation_end(),

    {ok, #state{actor=Actor, expected_result=ExpectedResult}}.

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
    {ok, SetDocIdToContents} = lasp:query(?SET_DOC_ID_TO_CONTENTS),
    lager:info("Number of Docs: ~p", [sets:size(SetDocIdToContents)]),

    %% Print an intermediate set.
    {ok, SetWordToDocCountAndNumOfDocs} =
        lasp:query(?SET_WORD_TO_DOC_COUNT_AND_NUM_OF_DOCS),
    lager:info(
        "SetWordToDocCountAndNumOfDocs: ~p", [SetWordToDocCountAndNumOfDocs]),

    %% Schedule advertisement counter impression.
    schedule_logging(),

    {noreply, State};

handle_info(
    check_simulation_end, #state{expected_result=ExpectedResult}=State) ->
    lasp_marathon_simulations:log_message_queue_size("check_simulation_end"),

    %% A simulation ends for the server when the result CRDT is same as the
    %% expected result.
    {ok, WordToDocFrequency} = lasp:query(?SET_WORD_TO_DOC_FREQUENCY),

    lager:info(
        "Checking for simulation end: current result: ~p.",
        [WordToDocFrequency]),

    {ok, CounterCompletedClients} = lasp:query(?COUNTER_COMPLETED_CLIENTS),

    lager:info(
        "Checking for simulation end: completed clients: ~p.",
        [CounterCompletedClients]),

    case WordToDocFrequency == ExpectedResult andalso
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
build_dag(SetDocIdToContents, SetWordToDocFrequency) ->
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

    ok =
        lasp:map(
            SetWordToDocCountAndNumOfDocs,
            fun({{Word, DocCount}, NumOfDocsSizeT}) ->
                NumOfDocs = NumOfDocsSizeT,
                {Word, DocCount / NumOfDocs}
            end,
            SetWordToDocFrequency),

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
